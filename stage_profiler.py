#!/usr/bin/env python3
"""
Knative cold-start stage profiler (+ wake-from-zero + rolling quantiles)
------------------------------------------------------------------------
• Profiles every new pod (robust LIST + WATCH).
• Derives all seven stages from the Kubernetes API.
• Stage 5 ends when the queue-proxy container becomes Ready.
• If API-server closes the stream, it reconnects.

Structured logs (files under --logs-dir, default ./logs):
  - stage_timings.ndjson        (append-only history; one JSON per coldstart/reprobe)
  - latest_and_best.json        ({"current": ..., "best_overall": ...})
  - quantiles.ndjson            (append-only 5-min window rollups per svc/rev/kind)
  - quantiles_latest.json       (latest rollups across keys)

New (wake-from-zero):
• If no pods match the selector (e.g., minScale=0), the profiler sends an HTTP request
  to the service URL to trigger scale-from-zero so a pod boots and is then monitored.

New (rolling quantiles over time):
• Computes P50 / P95 / P99 / P99.9 of total cold-start latency (total_ms) over time windows
  per (namespace, service, revision, kind), and writes to quantiles.ndjson + quantiles_latest.json.
"""

import os, json, logging, argparse, threading, time, pathlib, random
from collections import OrderedDict, defaultdict
from datetime import datetime, timezone
from urllib.parse import urlparse, urlencode
from typing import Optional, Tuple, Dict, List

import requests
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# ────────────  Logging  ────────────
level = os.getenv("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, level, logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("stage-profiler")

def _utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
def _ms(sec: float) -> str:
    return f"{sec*1e3:.2f} ms"
def _iso(dt: datetime) -> str:
    return _utc(dt).isoformat()

def _sanitize(endpoint: str) -> str:
    endpoint = (endpoint or "").rstrip("/v1/traces").rstrip("/v1/metrics").strip()
    return urlparse(endpoint).netloc if endpoint.startswith(("http://", "https://")) else endpoint

# ────── OpenTelemetry for profiler’s own spans ──────
collector_ep = _sanitize(os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or
                         os.getenv("OTEL_COLLECTOR_ENDPOINT") or
                         "localhost:4317")
tp = TracerProvider(resource=Resource({"service.name": "stage-profiler"}))
tp.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=collector_ep, insecure=True)))
trace.set_tracer_provider(tp)
tracer = trace.get_tracer(__name__)

# ────────────  Global state  ────────────
pod_state, pod_timings = {}, {}
pod_lock = threading.Lock()
ready_event = threading.Event()
ready_uid: Optional[str] = None
processed = set()
service_by_uid: Dict[str, str] = {}
pod_name_by_uid: Dict[str, str] = {}
namespace_by_uid: Dict[str, str] = {}
revision_by_uid: Dict[str, str] = {}

STAGES = OrderedDict([
    ("stage1-scheduling",      "Pod Scheduling"),
    ("stage2-image-pull",      "Image Pull"),
    ("stage3-container-init",  "Container Init"),
    ("stage5-runtime-startup", "Runtime Startup"),
    ("stage4-proxy-warmup",    "Proxy Warm-up"),
    ("stage6-app-init",        "App Init"),
    ("stage7-first-request",   "First Request"),
])

v1: client.CoreV1Api  # set later

# ────────────  File outputs  ────────────
HISTORY_LOG = None          # logs/stage_timings.ndjson
BEST_CURRENT_JSON = None    # logs/latest_and_best.json
QUANTILES_LOG = None        # logs/quantiles.ndjson
QUANTILES_LATEST_JSON = None# logs/quantiles_latest.json

def _ensure_logs_dir(logs_dir: str):
    path = pathlib.Path(logs_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path

def _write_line(path: str, obj: dict):
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception as e:
        log.warning("Failed writing %s: %s", path, e)

def _write_json(path: str, obj: dict):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning("Failed writing %s: %s", path, e)

_best_lock = threading.Lock()
_best_overall = None  # cached best by lowest total_ms (coldstarts only)

def _update_latest_and_best(current_record: dict):
    global _best_overall
    with _best_lock:
        cur_total = current_record.get("total_ms", float("inf"))
        if (current_record.get("kind") == "coldstart" and
            (_best_overall is None or cur_total < _best_overall.get("total_ms", float("inf")))):
            _best_overall = current_record
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "current": current_record,
            "best_overall": _best_overall,
        }
        _write_json(BEST_CURRENT_JSON, payload)

# ────────────  Quantiles over time (rolling windows)  ────────────

Q_WINDOW_SEC = int(os.getenv("QUANTILES_WINDOW_SEC", "300"))           # 5 minutes
Q_FLUSH_EVERY_SEC = int(os.getenv("QUANTILES_FLUSH_EVERY_SEC", "30"))  # check every 30s

# key: (ns, svc, rev, kind, window_start_epoch) -> list of total_ms
_q_lock = threading.Lock()
_q_buckets: Dict[Tuple[str, str, str, str, int], List[float]] = {}
_q_latest: Dict[Tuple[str, str, str, str], dict] = {}

def _to_epoch(ts_iso: str) -> int:
    try:
        dt = datetime.fromisoformat(ts_iso.replace("Z","+00:00"))
        return int(_utc(dt).timestamp())
    except Exception:
        return int(time.time())

def _window_start(ts_iso: str) -> int:
    t = _to_epoch(ts_iso)
    return t - (t % Q_WINDOW_SEC)

def _percentile(sorted_vals: List[float], p: float) -> Optional[float]:
    n = len(sorted_vals)
    if n == 0: return None
    if n == 1: return sorted_vals[0]
    # linear interpolation between closest ranks
    rank = (p/100.0) * (n - 1)
    lo = int(rank)
    hi = min(n - 1, lo + 1)
    frac = rank - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac

def quantiles_add(record: dict):
    """Add a single record (expects keys: timestamp, namespace, service, revision, kind, total_ms)."""
    if not record or record.get("kind") != "coldstart":
        return
    ns = record.get("namespace","unknown")
    svc = record.get("service","unknown")
    rev = record.get("revision","unknown")
    kind = record.get("kind","coldstart")
    ts_iso = record.get("timestamp")
    total = record.get("total_ms")
    try:
        total = float(total)
    except Exception:
        return
    w = _window_start(ts_iso or datetime.now(timezone.utc).isoformat())
    key = (ns, svc, rev, kind, w)
    with _q_lock:
        _q_buckets.setdefault(key, []).append(total)

def quantiles_worker():
    """Periodically flush closed windows to quantiles.ndjson and update quantiles_latest.json."""
    while True:
        time.sleep(Q_FLUSH_EVERY_SEC)
        now = int(time.time())
        cutoff = now - Q_WINDOW_SEC  # flush windows that ended before 'cutoff'
        out_any = False
        with _q_lock:
            for key in list(_q_buckets.keys()):
                ns, svc, rev, kind, w = key
                if w <= cutoff:
                    vals = _q_buckets.pop(key, [])
                    vals.sort()
                    n = len(vals)
                    p50  = _percentile(vals, 50.0)
                    p95  = _percentile(vals, 95.0)
                    p99  = _percentile(vals, 99.0)
                    p999 = _percentile(vals, 99.9)
                    window = {
                        "window_start": datetime.fromtimestamp(w, tz=timezone.utc).isoformat(),
                        "window_end":   datetime.fromtimestamp(w + Q_WINDOW_SEC, tz=timezone.utc).isoformat(),
                        "namespace": ns, "service": svc, "revision": rev, "kind": kind,
                        "count": n,
                        "p50_ms": round(p50 or 0.0, 3),
                        "p95_ms": round(p95 or 0.0, 3),
                        "p99_ms": round(p99 or 0.0, 3),
                        "p999_ms": round(p999 or 0.0, 3),
                    }
                    _write_line(QUANTILES_LOG, window)
                    _q_latest[(ns, svc, rev, kind)] = window
                    log.info("[quantiles] %s/%s rev=%s kind=%s count=%d P50=%.1f P95=%.1f P99=%.1f P99.9=%.1f",
                             ns, svc, rev, kind, n, window["p50_ms"], window["p95_ms"], window["p99_ms"], window["p999_ms"])
                    out_any = True
            if out_any:
                _write_json(QUANTILES_LATEST_JSON, {
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "window_sec": Q_WINDOW_SEC,
                    "latest": list(_q_latest.values()),
                })

# ────────────  Span + timing helpers  ────────────
def emit_span(stage: str, start: datetime, end: datetime, uid: str):
    dur_ms = (end - start).total_seconds()*1000
    # record timings for table + JSON
    with pod_lock:
        pod_timings.setdefault(uid, {})[stage] = (start, end, dur_ms)
    # emit span with useful attributes (for future OTel processing)
    span = tracer.start_span(stage, start_time=int(_utc(start).timestamp()*1e9))
    svc = service_by_uid.get(uid, "unknown")
    ns  = namespace_by_uid.get(uid, "unknown")
    rev = revision_by_uid.get(uid, "unknown")
    span.set_attribute("k8s.namespace.name", ns)
    span.set_attribute("knative.service",     svc)
    span.set_attribute("knative.revision",    rev)
    span.set_attribute("prof.stage_id",       stage)
    span.end(end_time=int(_utc(end).timestamp()*1e9))
    log.info("%s [%s]: %s", STAGES.get(stage, stage), uid[:6], _ms(dur_ms/1000))

def emit_total_span(uid: str, kind: str):
    """One span covering the entire cold start (for external histogramming, optional)."""
    with pod_lock:
        t = pod_timings.get(uid, {})
    if not t or "stage1-scheduling" not in t or "stage7-first-request" not in t:
        return
    start = t["stage1-scheduling"][0]
    end   = t["stage7-first-request"][1]
    span = tracer.start_span("coldstart-total", start_time=int(_utc(start).timestamp()*1e9))
    svc = service_by_uid.get(uid, "unknown")
    ns  = namespace_by_uid.get(uid, "unknown")
    rev = revision_by_uid.get(uid, "unknown")
    span.set_attribute("k8s.namespace.name", ns)
    span.set_attribute("knative.service",     svc)
    span.set_attribute("knative.revision",    rev)
    span.set_attribute("prof.kind",           kind)  # "coldstart" or "reprobe"
    span.end(end_time=int(_utc(end).timestamp()*1e9))

def print_table(uid: str):
    timings = pod_timings.get(uid)
    if not timings:
        return
    svc = service_by_uid.get(uid, "unknown")
    total = (timings["stage7-first-request"][1] - timings["stage1-scheduling"][0]).total_seconds()*1000
    print("\n\u250f" + "━"*70 + "┓")
    print(f"┃ {'COLD START PROFILE':^68} ┃")
    print(f"┃ Pod {uid[:8]}  Service {svc:<20} {'':>24} ┃")
    print("┣" + "━"*70 + "┫")
    print(f"┃ {'Stage':^30} │ {'Duration':^18} │ {'% Total':^15} ┃")
    print("┣" + "━"*30 + "╋" + "━"*18 + "╋" + "━"*15 + "┫")
    for sid, label in STAGES.items():
        if sid in timings:
            _, _, ms = timings[sid]
            pct = ms/total*100 if total else 0
            print(f"┃ {label:30} │ {_ms(ms/1000):18} │ {pct:13.1f}% ┃")
    print("┗" + "━"*70 + "┛\n")

# ───────  Stage-5 monitor (queue-proxy)  ───────
def monitor_qproxy(ns: str, pod_name: str, uid: str, sched_ts: datetime):
    for _ in range(120):
        try:
            pod = v1.read_namespaced_pod(pod_name, ns)
            for cs in (pod.status.container_statuses or []):
                if cs.name == "queue-proxy" and cs.ready:
                    emit_span("stage5-runtime-startup", sched_ts, datetime.now(timezone.utc), uid)
                    return
        except Exception:
            pass
        time.sleep(1)
    log.warning("queue-proxy never Ready for %s; skipping stage5", uid[:6])

# ───────  Pod event processing  ───────
def _svc_name(pod) -> Optional[str]:
    lbl = pod.metadata.labels or {}
    return (lbl.get("serving.knative.dev/service")
            or lbl.get("app.kubernetes.io/name")
            or lbl.get("app"))

def _rev_name(pod) -> Optional[str]:
    lbl = pod.metadata.labels or {}
    return lbl.get("serving.knative.dev/revision")

def process_pod(pod):
    global ready_uid
    uid, name, ns = pod.metadata.uid, pod.metadata.name, pod.metadata.namespace
    if uid in processed:
        return

    pod_name_by_uid[uid] = name
    namespace_by_uid[uid] = ns
    if (rev := _rev_name(pod)):
        revision_by_uid[uid] = rev

    log.info("Saw pod %s in %s (phase=%s)", name, ns, pod.status.phase)

    st = pod_state.setdefault(uid, {"namespace": ns, "name": name})
    st.setdefault("submit", pod.metadata.creation_timestamp)

    # Stage 1: PodScheduled -> True
    for c in (pod.status.conditions or []):
        if c.type == "PodScheduled" and c.status == "True" and "scheduled" not in st:
            st["scheduled"] = c.last_transition_time
            emit_span("stage1-scheduling", st["submit"], c.last_transition_time, uid)
            if (svc := _svc_name(pod)):
                service_by_uid[uid] = svc

    # Stage 2: first container running timestamp
    if "scheduled" in st and "running" not in st:
        for cs in (pod.status.container_statuses or []):
            if cs.state and cs.state.running:
                st["running"] = cs.state.running.started_at
                emit_span("stage2-image-pull", st["scheduled"], st["running"], uid)
                break

    # Stage 3: Pod Ready / ContainersReady
    for c in (pod.status.conditions or []):
        if c.type in ("Ready", "ContainersReady") and c.status == "True":
            if "ready" not in st and "running" in st:
                st["ready"] = c.last_transition_time
                emit_span("stage3-container-init", st["running"], c.last_transition_time, uid)
                threading.Thread(target=monitor_qproxy,
                                 args=(ns, name, uid, st["scheduled"]),
                                 daemon=True).start()
                ready_uid = uid
                ready_event.set()
            break

# ───────  Kubernetes watch loop (robust)  ───────
def setup_k8s():
    global v1
    try:
        config.load_incluster_config()
        log.info("Loaded in-cluster config")
    except Exception:
        config.load_kube_config()
        log.info("Loaded kubeconfig")
    v1 = client.CoreV1Api()

def list_matching_pods(ns: str, selector: str):
    return v1.list_namespaced_pod(ns, label_selector=selector or "")

def watch_pods(ns: str, selector: str):
    setup_k8s()
    # 1) Initial LIST
    resp = list_matching_pods(ns, selector)
    log.info("Initial list: %d pod(s) matched selector '%s'", len(resp.items), selector or "<all>")
    for p in resp.items:
        process_pod(p)
    rv = resp.metadata.resource_version
    # 2) WATCH
    while True:
        w = watch.Watch()
        try:
            for ev in w.stream(
                v1.list_namespaced_pod,
                namespace=ns,
                label_selector=selector or "",
                resource_version=rv,
                timeout_seconds=300,
                allow_watch_bookmarks=True,
            ):
                et = ev["type"]
                obj = ev["object"]
                if et == "BOOKMARK":
                    rv = obj.metadata.resource_version
                    continue
                rv = obj.metadata.resource_version
                if et in ("ADDED", "MODIFIED"):
                    process_pod(obj)
        except Exception as exc:
            log.warning("pod watch closed (%s) – reconnect in 2 s", exc)
            time.sleep(2)
        finally:
            w.stop()

# ───────  Probe helpers (Stages 4/6/7 + wake)  ───────
def probe(url: str, uid: str, ready_ts: datetime, kind: str = "coldstart"):
    t0 = datetime.now(timezone.utc)
    for _ in range(60):
        try:
            if requests.get(url, timeout=2).status_code == 200:
                break
        except requests.RequestException:
            pass
        time.sleep(0.5)
    t4_end = datetime.now(timezone.utc)
    emit_span("stage4-proxy-warmup", t0, t4_end, uid)
    emit_span("stage6-app-init", ready_ts, t4_end, uid)

    t7 = datetime.now(timezone.utc)
    try:
        status = requests.get(url, timeout=5).status_code
    except requests.RequestException:
        status = -1
    emit_span("stage7-first-request", t7, datetime.now(timezone.utc), uid)
    log.info("[%s] First request HTTP status: %s", kind, status)

def _extract_knative_service_from_selector(selector: str) -> Optional[str]:
    if not selector:
        return None
    parts = [p.strip() for p in selector.split(",")]
    for p in parts:
        if p.startswith("serving.knative.dev/service="):
            return p.split("=", 1)[1]
    return None

def _build_probe_url(ns: str, selector: str, explicit_url: Optional[str]) -> Optional[str]:
    if explicit_url:
        return explicit_url
    svc = _extract_knative_service_from_selector(selector)
    if svc:
        return f"http://{svc}.{ns}.svc.cluster.local"
    return None

def wake_service(url: Optional[str], retries: int, interval_s: int):
    if not url:
        log.info("[wake] no URL to wake")
        return
    for i in range(max(1, retries)):
        try:
            q = {"wake": int(time.time()), "r": random.randint(0, 1_000_000)}
            sep = "&" if "?" in url else "?"
            target = f"{url}{sep}{urlencode(q)}"
            log.info("[wake] hitting %s (attempt %d/%d)", target, i+1, retries)
            requests.get(target, timeout=5)
        except requests.RequestException:
            pass
        if i < retries - 1:
            time.sleep(interval_s)

# ───────  Periodic cold-start trigger (delete a pod or wake if zero)  ───────
def periodic_coldstart(ns: str, selector: str, period_sec: int, wake_url: Optional[str],
                       wake_retries: int, wake_interval_s: int, jitter_sec: int = 10):
    if period_sec <= 0:
        return
    setup_k8s()
    time.sleep(min(5, period_sec))
    while True:
        try:
            pods = list_matching_pods(ns, selector)
            candidates = [p for p in pods.items if (p.status.phase in ("Running", "Pending"))]
            if not candidates:
                log.info("[periodic] no pods to delete; likely scaled to zero — sending wake")
                wake_service(wake_url, retries=wake_retries, interval_s=wake_interval_s)
            else:
                victim = random.choice(candidates)
                log.info("[periodic] Deleting pod %s to force cold start …", victim.metadata.name)
                try:
                    v1.delete_namespaced_pod(victim.metadata.name, ns, grace_period_seconds=0)
                except ApiException as e:
                    log.warning("[periodic] Failed to delete pod %s: %s", victim.metadata.name, e)
        except Exception as e:
            log.warning("[periodic] Error during periodic cold start: %s", e)

        sleep_s = period_sec + random.randint(0, max(1, jitter_sec))
        time.sleep(sleep_s)

# ───────  Zero-watcher: wake when there are zero pods  ───────
def zero_watcher(ns: str, selector: str, wake_url: Optional[str],
                 wake_enabled: bool, wake_interval_s: int, wake_retries: int):
    if not wake_enabled:
        return
    setup_k8s()
    time.sleep(3)
    while True:
        try:
            pods = list_matching_pods(ns, selector)
            if len(pods.items) == 0:
                log.info("[zero] 0 pods for selector='%s' — sending wake", selector)
                wake_service(wake_url, retries=wake_retries, interval_s=wake_interval_s)
        except Exception as e:
            log.warning("[zero] error checking pod count: %s", e)
        time.sleep(wake_interval_s)

# ───────  JSON serialization ───────
def _serialize_record(uid: str, kind: str = "coldstart") -> dict:
    with pod_lock:
        timings = pod_timings.get(uid, {}).copy()
        name = pod_state.get(uid, {}).get("name", pod_name_by_uid.get(uid))
        ns = pod_state.get(uid, {}).get("namespace", namespace_by_uid.get(uid))
        svc = service_by_uid.get(uid, "unknown")
        rev = revision_by_uid.get(uid, "unknown")

    if not timings:
        return {}

    try:
        total_ms = (timings["stage7-first-request"][1] - timings["stage1-scheduling"][0]).total_seconds()*1000
    except Exception:
        total_ms = sum(v[2] for v in timings.values())

    stages = {}
    for sid, label in STAGES.items():
        if sid in timings:
            start, end, ms = timings[sid]
            stages[sid] = {
                "label": label,
                "start": _iso(start),
                "end": _iso(end),
                "duration_ms": round(ms, 3),
            }

    return {
        "kind": kind,  # "coldstart" or "reprobe"
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "namespace": ns,
        "service": svc,
        "revision": rev,               # <— include revision
        "pod_uid": uid,
        "pod_name": name,
        "total_ms": round(total_ms, 3),
        "stages": stages,
    }

# ───────  main  ───────
def main():
    ap = argparse.ArgumentParser("Cold-start profiler (every pod + periodic cold starts + wake-from-zero + quantiles)")
    ap.add_argument("--namespace", required=True, help="Namespace to watch (e.g., default)")
    ap.add_argument("--selector", required=True, help="Label selector (e.g., 'serving.knative.dev/service=httpbin')")
    ap.add_argument("--url", required=False, help="Route URL to probe/wake; if omitted, tries 'http://<ksvc>.<ns>.svc.cluster.local'")
    ap.add_argument("--logs-dir", default="logs", help="Directory to write logs (default: ./logs)")
    ap.add_argument("--reprobe-every", type=int, default=0,
                    help="If >0, periodically re-probe the same URL (steady-state) and log entries as kind='reprobe' every N seconds (NOT a cold start).")
    ap.add_argument("--periodic-coldstart-every-minutes", type=int, default=20,
                    help="If >0, every N minutes delete a current pod; if none, wake the service to trigger a new cold start.")
    # wake-from-zero controls
    ap.add_argument("--wake-when-zero", dest="wake_when_zero", action="store_true", default=True,
                    help="Send requests to wake the service when no pods are present (default: on)")
    ap.add_argument("--no-wake-when-zero", dest="wake_when_zero", action="store_false",
                    help="Disable wake-from-zero behavior")
    ap.add_argument("--wake-interval-seconds", type=int, default=15, help="Seconds between wake attempts/checks when zero pods are present")
    ap.add_argument("--wake-retries", type=int, default=3, help="Number of wake HTTP attempts per wake cycle")
    args = ap.parse_args()

    # Prepare file paths
    logs_path = _ensure_logs_dir(args.logs_dir)
    global HISTORY_LOG, BEST_CURRENT_JSON, QUANTILES_LOG, QUANTILES_LATEST_JSON
    HISTORY_LOG = str(logs_path / "stage_timings.ndjson")
    BEST_CURRENT_JSON = str(logs_path / "latest_and_best.json")
    QUANTILES_LOG = str(logs_path / "quantiles.ndjson")
    QUANTILES_LATEST_JSON = str(logs_path / "quantiles_latest.json")

    # Start the quantiles worker
    threading.Thread(target=quantiles_worker, daemon=True).start()

    # Compute a wake/probe URL usable even when scaled to zero
    wake_probe_url = _build_probe_url(args.namespace, args.selector, args.url)
    if wake_probe_url:
        log.info("Wake/Probe URL: %s", wake_probe_url)
    else:
        log.warning("No wake/probe URL could be derived. Provide --url or include serving.knative.dev/service=<name> in --selector.")

    # Start the watch thread
    threading.Thread(target=watch_pods, args=(args.namespace, args.selector), daemon=True).start()

    # Start the periodic cold-start thread (delete or wake)
    period_sec = max(0, args.periodic_coldstart_every_minutes) * 60
    if period_sec > 0:
        threading.Thread(target=periodic_coldstart,
                         args=(args.namespace, args.selector, period_sec, wake_probe_url,
                               args.wake_retries, args.wake_interval_seconds),
                         daemon=True).start()
        log.info("Periodic cold-start enabled: every %d minutes", args.periodic_coldstart_every_minutes)
    else:
        log.info("Periodic cold-start disabled")

    # Start zero watcher (wake when 0 pods)
    threading.Thread(target=zero_watcher,
                     args=(args.namespace, args.selector, wake_probe_url,
                           args.wake_when_zero, args.wake_interval_seconds, args.wake_retries),
                     daemon=True).start()

    log.info("Profiler running in ns=%s selector='%s' (URL=%s)", args.namespace, args.selector, wake_probe_url or "<none>")

    # Optional steady-state re-probe loop state
    next_reprobe_at = 0
    last_probe_target: Optional[Tuple[str, str, datetime]] = None  # (url, uid, ready_ts)

    while True:
        # If a pod just became Ready, profile it (coldstart)
        if ready_event.wait(timeout=1.0):
            uid = None
            try:
                uid = ready_uid
            finally:
                ready_event.clear()
            if uid and uid not in processed:
                processed.add(uid)

                st = pod_state.get(uid, {})
                ready_ts = st.get("ready") or st.get("scheduled") or datetime.now(timezone.utc)

                probe_url = wake_probe_url
                if probe_url:
                    try:
                        probe(probe_url, uid, ready_ts, kind="coldstart")
                    except Exception as e:
                        log.warning("Probe failed for %s: %s", uid[:6], e)

                # Wait up to 120s for stage 5 to be recorded (queue-proxy ready)
                deadline = time.time() + 120
                while ("stage5-runtime-startup" not in pod_timings.get(uid, {}) and time.time() < deadline):
                    time.sleep(0.5)

                print_table(uid)

                # Persist results — history + latest/best
                record = _serialize_record(uid, kind="coldstart")
                if record:
                    _write_line(HISTORY_LOG, record)
                    _update_latest_and_best(record)
                    quantiles_add(record)         # <-- feed rolling quantiles
                    emit_total_span(uid, "coldstart")

                # Enable re-probe loop if wanted
                if args.reprobe_every > 0 and probe_url:
                    last_probe_target = (probe_url, uid, ready_ts)
                    next_reprobe_at = time.time() + args.reprobe_every

                log.info("Done with pod %s — waiting for next Ready pod …", uid[:6])

        # Steady-state re-probe (optional, NOT a cold start)
        if args.reprobe_every > 0 and last_probe_target and time.time() >= next_reprobe_at:
            probe_url, uid, ready_ts = last_probe_target
            alias_uid = f"{uid}-reprobe-{int(time.time())}"
            with pod_lock:
                pod_timings[alias_uid] = {}
                service_by_uid[alias_uid] = service_by_uid.get(uid, "unknown")
                namespace_by_uid[alias_uid] = namespace_by_uid.get(uid, "unknown")
                revision_by_uid[alias_uid] = revision_by_uid.get(uid, "unknown")
                pod_state[alias_uid] = pod_state.get(uid, {}).copy()
            try:
                probe(probe_url, alias_uid, ready_ts, kind="reprobe")
            except Exception as e:
                log.warning("Re-probe failed: %s", e)

            record = _serialize_record(alias_uid, kind="reprobe")
            if record:
                _write_line(HISTORY_LOG, record)
                _update_latest_and_best(record)
                # (intentionally not added to quantiles: reprobe != coldstart)
                emit_total_span(alias_uid, "reprobe")

            next_reprobe_at = time.time() + args.reprobe_every

if __name__ == "__main__":
    main()
