import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

from django.core import serializers
from django.core.exceptions import FieldError
from django.db import connection
from flask import Flask, Response, jsonify, request
from peeringdb import get_backend, resource
from peeringdb._update import Updater
from peeringdb.client import Client
from peeringdb.fetch import Fetcher
from werkzeug.exceptions import HTTPException
from werkzeug.middleware.proxy_fix import ProxyFix

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
app = Flask(__name__)
log = logging.getLogger("peeringdb-cache")

DATABASE_PATH = os.environ.get("DATABASE_PATH", "/data/peeringdb.sqlite3")
SYNC_INTERVAL = float(os.environ.get("SYNC_INTERVAL", 6 * 60 * 60))
API_KEY = os.environ.get("PEERINGDB_API_KEY", "")
SYNC_ONLY = [t for t in os.environ.get("PDB_SYNC_ONLY", "").split(",") if t]
# lightweight change detection between full syncs; 0 disables
CHANGE_CHECK_INTERVAL = float(os.environ.get("CHANGE_CHECK_INTERVAL", "600"))
# optionally limit probes to a subset of resource tags (comma-separated);
# empty = probe every synced resource
CHANGE_CHECK_RESOURCES = [
    t for t in os.environ.get("CHANGE_CHECK_RESOURCES", "").split(",") if t
]

PDB_CONFIG = {
    "orm": {
        "backend": "django_peeringdb",
        "database": {
            "engine": "sqlite3",
            "host": "",
            "name": DATABASE_PATH,
            "password": "",
            "port": 0,
            "user": "",
            "options": {"timeout": 30},
        },
        "migrate": True,
        "secret_key": "",
    },
    "sync": {
        "api_key": API_KEY,
        # public.peeringdb.com serves bulk per-resource files; much faster than
        # pulling the full dataset through the API on the initial sync
        "cache_url": os.environ.get("PDB_SYNC_CACHE_URL", "https://public.peeringdb.com"),
        "only": SYNC_ONLY,
        "password": "",
        "strip_tz": 1,
        "timeout": int(os.environ.get("PEERINGDB_SYNC_TIMEOUT", "60")),
        "url": os.environ.get("PDB_SYNC_URL", "https://www.peeringdb.com/api"),
        "user": "",
    },
}

pdb = Client(cfg=PDB_CONFIG)

# WAL mode lets readers proceed while a sync holds the write transaction
# ("database is locked" errors otherwise)
try:
    with connection.cursor() as cur:
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
except Exception:
    log.exception("failed to set sqlite pragmas")


class SyncManager:
    """Serializes sync runs so the scheduler and /sync can't overlap."""

    def __init__(self):
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._pending = True  # run a sync immediately on startup
        self.running = False
        self.last_sync = 0.0
        self.last_duration = 0.0
        self.last_error = None
        self.next_sync = 0.0
        self.sync_total = 0
        self.consecutive_failures = 0
        self.last_change_check = 0.0
        self.change_detected_total = 0

    def trigger(self):
        """Kick off a sync now. Returns False if one is running or queued."""
        with self._lock:
            if self.running or self._pending:
                return False
            self._pending = True
        self._wake.set()
        return True

    def _run_sync(self):
        with self._lock:
            if self.running:
                return
            self.running = True
        with self._lock:
            self._pending = False
        started = time.time()
        try:
            log.info("sync started")
            # A fresh Fetcher per run: Fetcher keeps fetched resources in memory
            # for its lifetime, so reusing it makes every sync after the first
            # a silent no-op against stale data.
            updater = Updater(Fetcher(**PDB_CONFIG["sync"]))
            # poc/ixlan only carry contact data when fetched privately
            updater.update_all(self._resources(), fetch_private=bool(API_KEY))
        except Exception as e:
            self.consecutive_failures += 1
            self.last_error = f"{type(e).__name__}: {e}"
            # backoff: 1m, 2m, 4m ... capped at the normal interval
            self.next_sync = time.time() + min(
                60 * 2 ** self.consecutive_failures, SYNC_INTERVAL
            )
            log.exception(
                "sync failed (attempt %d), retrying at %s",
                self.consecutive_failures,
                time.strftime("%H:%M:%S", time.localtime(self.next_sync)),
            )
        else:
            self.last_sync = time.time()
            self.last_duration = self.last_sync - started
            self.last_error = None
            self.consecutive_failures = 0
            self.sync_total += 1
            self.next_sync = self.last_sync + SYNC_INTERVAL
            # a fresh sync is also a fresh change check
            self.last_change_check = self.last_sync
            log.info("sync finished in %.1fs", self.last_duration)
        finally:
            with self._lock:
                self.running = False

    def _resources(self):
        if SYNC_ONLY:
            return [resource.get_resource(t) for t in SYNC_ONLY]
        return resource.all_resources()

    def _check_resources(self):
        if CHANGE_CHECK_RESOURCES:
            return [resource.get_resource(t) for t in CHANGE_CHECK_RESOURCES]
        return self._resources()

    @staticmethod
    def _row_updated(row):
        """Epoch seconds for a remote row's `updated` ISO timestamp."""
        u = row.get("updated")
        if not u:
            return 0
        try:
            dt = datetime.fromisoformat(str(u).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            return 0

    def _check_for_changes(self):
        """Probe each synced resource with a ?since= query and return True if
        the remote has objects newer than our local data. Responses are ~30
        bytes when nothing changed, so a check cycle is very cheap."""
        b = get_backend()
        fetcher = Fetcher(**PDB_CONFIG["sync"])
        for res in self._check_resources():
            concrete = b.get_concrete(res)
            since = b.last_change(concrete)
            if not since:
                # empty local table: everything remote is new
                return True
            # the -1 lookback re-fetches boundary rows, so compare each row's
            # `updated` against our watermark instead of just non-emptiness
            fetcher.load(res.tag, since - 1, fetch_private=bool(API_KEY))
            if any(
                self._row_updated(r) > since for r in fetcher.entries(res.tag)
            ):
                return True
        return False

    def scheduler_loop(self):
        while True:
            try:
                if not self.running:
                    due_sync = self._pending or time.time() >= self.next_sync
                    due_check = (
                        not due_sync
                        and CHANGE_CHECK_INTERVAL > 0
                        # don't probe while backing off from failures; the
                        # remote may be the thing that's broken
                        and self.consecutive_failures == 0
                        and time.time() - self.last_change_check
                        >= CHANGE_CHECK_INTERVAL
                    )
                    if due_sync:
                        self._run_sync()
                    elif due_check:
                        self.last_change_check = time.time()
                        if self._check_for_changes():
                            self.change_detected_total += 1
                            log.info("remote changes detected, starting sync")
                            self._run_sync()
            except Exception:
                # the scheduler must never die; belt-and-braces since
                # _run_sync already catches everything
                log.exception("scheduler error")
            self._wake.wait(timeout=30)
            self._wake.clear()


sync_mgr = SyncManager()
sched = threading.Thread(
    target=sync_mgr.scheduler_loop, name="sync-scheduler", daemon=True
)
sched.start()


def _concrete(model):
    return get_backend().get_concrete(model)


def _fk_fields(concrete):
    return [f.name for f in concrete._meta.fields if f.is_relation and f.many_to_one]


def serialize_queryset(qs, model):
    """Serialize a queryset in one pass and patch in API-compatible fields."""
    concrete = _concrete(model)
    fks = _fk_fields(concrete)
    entries = json.loads(serializers.serialize("json", qs.iterator()))

    # netixlan: the real API denormalizes the IX name/id onto the object
    ixlan_names = {}
    if model is resource.NetworkIXLan and entries:
        ixlan_ids = {e["fields"].get("ixlan") for e in entries}
        ixlan_ids.discard(None)
        for ixlan in concrete.ixlan.field.related_model.objects.filter(
            pk__in=ixlan_ids
        ).select_related("ix"):
            ixlan_names[ixlan.pk] = (ixlan.ix_id, ixlan.ix.name if ixlan.ix else "")

    out = []
    for e in entries:
        fields = e["fields"]
        fields["id"] = e["pk"]
        for fk in fks:
            if fk in fields:
                fields[f"{fk}_id"] = fields[fk]
        if model is resource.NetworkIXLan:
            ix_id, name = ixlan_names.get(fields.get("ixlan"), (0, ""))
            fields["ix_id"] = ix_id
            fields["name"] = name
        out.append(fields)
    return out


def parse_args():
    args = {}
    for k, v in request.args.items():
        # Attempt int conversion
        try:
            args[k] = int(v)
        except ValueError:
            args[k] = v
    return args


# query params that don't map to a model field directly; the real API
# accepts e.g. ?ix_id= on netixlan even though it's stored as ixlan__ix
FIELD_ALIASES = {
    "netixlan": {"ix": "ixlan__ix", "ix_id": "ixlan__ix"},
    "ixpfx": {"ix": "ixlan__ix", "ix_id": "ixlan__ix"},
}


def find_by_args(model):
    args = parse_args()
    concrete = _concrete(model)
    field_names = {f.name for f in concrete._meta.fields}
    qs = concrete.objects.all()

    for k, v in args.items():
        # real API accepts both `net` and `net_id` style params; strip a
        # trailing _id so both work against the Django fields
        field = FIELD_ALIASES.get(model.tag, {}).get(k, k)
        if field.endswith("_id") and field[:-3] in field_names:
            field = field[:-3]
        try:
            qs = qs.filter(**{field: v})
        except (FieldError, ValueError):
            return jsonify({"meta": {"error": f"invalid filter: {k}"}, "data": []}), 400

    try:
        data = serialize_queryset(qs, model)
    except (FieldError, ValueError) as e:
        return jsonify({"meta": {"error": str(e)}, "data": []}), 400
    return jsonify({"data": data})


@app.route("/api/org", methods=["GET"])
def get_org():
    return find_by_args(resource.Organization)


@app.route("/api/fac", methods=["GET"])
def get_fac():
    return find_by_args(resource.Facility)


@app.route("/api/net", methods=["GET"])
def get_net():
    return find_by_args(resource.Network)


@app.route("/api/ix", methods=["GET"])
def get_ix():
    return find_by_args(resource.InternetExchange)


@app.route("/api/campus", methods=["GET"])
def get_campus():
    return find_by_args(resource.Campus)


@app.route("/api/carrier", methods=["GET"])
def get_carrier():
    return find_by_args(resource.Carrier)


@app.route("/api/carrierfac", methods=["GET"])
def get_carrierfac():
    return find_by_args(resource.CarrierFacility)


@app.route("/api/ixfac", methods=["GET"])
def get_ixfac():
    return find_by_args(resource.InternetExchangeFacility)


@app.route("/api/ixlan", methods=["GET"])
def get_ixlan():
    return find_by_args(resource.InternetExchangeLan)


@app.route("/api/ixpfx", methods=["GET"])
def get_ixpfx():
    return find_by_args(resource.InternetExchangeLanPrefix)


@app.route("/api/netfac", methods=["GET"])
def get_netfac():
    return find_by_args(resource.NetworkFacility)


@app.route("/api/netixlan", methods=["GET"])
def get_netixlan():
    return find_by_args(resource.NetworkIXLan)


@app.route("/api/poc", methods=["GET"])
def get_poc():
    return find_by_args(resource.NetworkContact)


RESOURCES = [
    ("orgs", resource.Organization),
    ("fac", resource.Facility),
    ("net", resource.Network),
    ("ix", resource.InternetExchange),
    ("campus", resource.Campus),
    ("carrier", resource.Carrier),
    ("carrierfac", resource.CarrierFacility),
    ("ixfac", resource.InternetExchangeFacility),
    ("ixlan", resource.InternetExchangeLan),
    ("ixpfx", resource.InternetExchangeLanPrefix),
    ("netfac", resource.NetworkFacility),
    ("netixlan", resource.NetworkIXLan),
    ("poc", resource.NetworkContact),
]


@app.route("/metrics")
def metrics():
    counts = {}
    try:
        for tag, res in RESOURCES:
            # count() issues SELECT COUNT(*) rather than materializing rows
            counts[tag] = pdb.all(res).count()
    except Exception:
        log.exception("metrics count failed")

    lines = [
        "# HELP peeringdb_cache_entries Number of entries in the PeeringDB cache",
        "# TYPE peeringdb_cache_entries gauge",
    ]
    lines += [f'peeringdb_cache_entries{{model="{t}"}} {c}' for t, c in counts.items()]
    lines += [
        "",
        "# HELP peeringdb_sync_running Is a sync task running?",
        "# TYPE peeringdb_sync_running gauge",
        f"peeringdb_sync_running {int(sync_mgr.running)}",
        "",
        "# HELP peeringdb_last_sync Epoch of last successful sync",
        "# TYPE peeringdb_last_sync gauge",
        f"peeringdb_last_sync {int(sync_mgr.last_sync)}",
        "",
        "# HELP peeringdb_next_sync Epoch of next scheduled sync",
        "# TYPE peeringdb_next_sync gauge",
        f"peeringdb_next_sync {int(sync_mgr.next_sync)}",
        "",
        "# HELP peeringdb_sync_duration_seconds Duration of the last successful sync",
        "# TYPE peeringdb_sync_duration_seconds gauge",
        f"peeringdb_sync_duration_seconds {sync_mgr.last_duration:.3f}",
        "",
        "# HELP peeringdb_sync_total Number of successful syncs since startup",
        "# TYPE peeringdb_sync_total counter",
        f"peeringdb_sync_total {sync_mgr.sync_total}",
        "",
        "# HELP peeringdb_sync_consecutive_failures Failed syncs in a row",
        "# TYPE peeringdb_sync_consecutive_failures gauge",
        f"peeringdb_sync_consecutive_failures {sync_mgr.consecutive_failures}",
        "",
        "# HELP peeringdb_last_change_check Epoch of last remote change probe",
        "# TYPE peeringdb_last_change_check gauge",
        f"peeringdb_last_change_check {int(sync_mgr.last_change_check)}",
        "",
        "# HELP peeringdb_change_detected_total Probes that found remote changes",
        "# TYPE peeringdb_change_detected_total counter",
        f"peeringdb_change_detected_total {sync_mgr.change_detected_total}",
        "",
        "# HELP peeringdb_scheduler_alive Is the sync scheduler thread alive?",
        "# TYPE peeringdb_scheduler_alive gauge",
        f"peeringdb_scheduler_alive {int(sched.is_alive())}",
        "",
    ]
    return Response("".join(f"{l}\n" for l in lines), mimetype="text/plain")


@app.route("/sync", methods=["GET", "POST"])
def sync_start():
    if sync_mgr.trigger():
        return jsonify({"started": True}), 202
    return jsonify({"started": False}), 409


@app.route("/healthz")
def healthz():
    """Liveness: process is up and the scheduler thread hasn't died."""
    alive = sched.is_alive()
    return jsonify({"status": "ok" if alive else "dead", "scheduler_alive": alive}), (
        200 if alive else 503
    )


@app.route("/readyz")
def readyz():
    """Readiness: database is reachable and holds data."""
    try:
        has_data = pdb.all(resource.Organization).exists()
    except Exception:
        log.exception("readiness check failed")
        return jsonify({"ready": False}), 503
    return jsonify(
        {
            "ready": has_data,
            "last_sync": sync_mgr.last_sync,
            "sync_running": sync_mgr.running,
            "last_error": sync_mgr.last_error,
        }
    ), (200 if has_data else 503)


@app.errorhandler(Exception)
def handle_error(e):
    if isinstance(e, HTTPException):
        return e
    log.exception("unhandled error: %s %s", request.method, request.path)
    return jsonify({"meta": {"error": str(e)}, "data": []}), 500


# trust X-Forwarded-For etc. only when explicitly told we're behind a proxy;
# PROXY_FIX_COUNT is the number of trusted proxies in front of the app
_proxy_count = int(os.environ.get("PROXY_FIX_COUNT", "0"))
if _proxy_count > 0:
    app.wsgi_app = ProxyFix(
        app.wsgi_app, x_for=_proxy_count, x_proto=_proxy_count, x_host=_proxy_count
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)
