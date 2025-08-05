#!/usr/bin/env python3
"""
Knative cold-start stage profiler (loops forever, auto-detects service name)
--------------------------------------------------------------------------
Uses only the Kubernetes API to time every phase of a Knative cold start,
including Stage 5 (runtime start-up via queue-proxy ready).  Results are printed
as a formatted table after each pod is profiled.
"""

import os
import logging
import argparse
import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from kubernetes import client, config, watch
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# ─────────────────────────  Logging  ─────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("stage-profiler")

# ───────────────────  Helper functions  ──────────────────────
def _strip_suffix(txt: str, suf: str) -> str:
    return txt[:-len(suf)] if txt.endswith(suf) else txt


def _sanitize_endpoint(raw: str) -> str:
    raw = _strip_suffix(_strip_suffix(raw.strip(), "/v1/traces"), "/v1/metrics")
    if raw.startswith(("http://", "https://")):
        raw = urlparse(raw).netloc
    return raw or "localhost:4317"


def _utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _ms(sec: float) -> str:
    return f"{sec * 1e3:.2f} ms"

# ───────────────────  OpenTelemetry setup  ───────────────────
collector_ep = _sanitize_endpoint(
    os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    or os.getenv("OTEL_COLLECTOR_ENDPOINT")
    or "localhost:4317"
)
provider = TracerProvider(resource=Resource({"service.name": "stage-profiler"}))
provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint=collector_ep, insecure=True))
)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

# ───────────────  Global mutable state  ──────────────────────
pod_state: dict[str, dict] = {}
pod_lock = threading.Lock()
ready_event = threading.Event()
ready_uid: str | None = None
scheduled_ts: dict[str, datetime] = {}
service_by_uid: dict[str, str] = {}

processed: set[str] = set()
pod_timings: dict[str, dict] = {}

STAGES = OrderedDict([
    ("stage1-scheduling",      "Pod Scheduling"),
    ("stage2-image-pull",      "Image Pull"),
    ("stage3-container-init",  "Container Init"),
    ("stage5-runtime-startup", "Runtime Startup"),
    ("stage4-proxy-warmup",    "Proxy Warmup"),
    ("stage6-app-init",        "App Init"),
    ("stage7-first-request",   "First Request"),
])

v1_client: client.CoreV1Api | None = None

# ────────────────  Span emitter  ─────────────────────────────
def emit_span(name: str, start: datetime, end: datetime, uid: str):
    if end <= start:
        end = start + timezone.utc.utcoffset(start) + \
              timezone.utc.utcoffset(start)  # ensure >0 ms
    span = tracer.start_span(name, start_time=int(_utc(start).timestamp() * 1e9))
    span.end(end_time=int(_utc(end).timestamp() * 1e9))
    duration_ms = (end - start).total_seconds() * 1000

    with pod_lock:
        pod_timings.setdefault(uid, {})[name] = (start, end, duration_ms)
    log.info("%s [%s]: %s", STAGES.get(name, name), uid[:6], _ms(duration_ms / 1000))

# ───────────────  Pretty-print results  ─────────────────────
def print_profile_results(uid: str):
    with pod_lock:
        timings = pod_timings.get(uid)
    if not timings:
        return

    svc = service_by_uid.get(uid, "unknown")
    total_ms = (
        (timings["stage7-first-request"][1] - timings["stage1-scheduling"][0]).total_seconds()
        * 1000
        if {"stage1-scheduling", "stage7-first-request"} <= timings.keys()
        else 0
    )

    print("\n┏" + "━" * 70 + "┓")
    print(f"┃ {'COLD START PROFILE':^68} ┃")
    print(f"┃ {'Pod: ' + uid[:8] + '  Service: ' + svc:^68} ┃")
    print("┣" + "━" * 70 + "┫")
    print(f"┃ {'Stage':^30} │ {'Duration':^18} │ {'% of Total':^15} ┃")
    print("┠" + "─" * 30 + "┼" + "─" * 18 + "┼" + "─" * 15 + "┨")

    for stage_id, stage_name in STAGES.items():
        if stage_id in timings:
            _, _, dur = timings[stage_id]
            pct = dur / total_ms * 100 if total_ms else 0
            print(f"┃ {stage_name:30} │ {_ms(dur/1000):18} │ {pct:13.1f}% ┃")

    print("┗" + "━" * 70 + "┛\n")

# ──────────────  Stage 5 monitor (queue-proxy)  ─────────────
def monitor_queue_proxy_ready(namespace, pod_name, uid, scheduled_time):
    global v1_client
    for _ in range(120):
        try:
            pod = v1_client.read_namespaced_pod(pod_name, namespace)
            for cs in pod.status.container_statuses or []:
                if cs.name == "queue-proxy" and cs.ready:
                    emit_span("stage5-runtime-startup",
                              scheduled_time, datetime.now(timezone.utc), uid)
                    return
        except Exception as e:
            log.debug("queue-proxy check error: %s", e)
        time.sleep(1)
    log.warning("queue-proxy not ready for pod %s; skipping stage5", uid[:6])

# ───────────────  Pod event processor  ──────────────────────
def _detect_service_name(pod) -> str | None:
    lbl = pod.metadata.labels or {}
    return (lbl.get("serving.knative.dev/service")
            or lbl.get("app.kubernetes.io/name")
            or lbl.get("app"))

def process_pod(pod):
    global ready_uid
    uid, name, ns = pod.metadata.uid, pod.metadata.name, pod.metadata.namespace
    if uid in processed:
        return

    state = pod_state.setdefault(uid, {})
    state.setdefault("submit", pod.metadata.creation_timestamp)

    # Stage 1
    for cond in pod.status.conditions or []:
        if cond.type == "PodScheduled" and cond.status == "True" and "scheduled" not in state:
            state["scheduled"] = cond.last_transition_time
            scheduled_ts[uid] = cond.last_transition_time
            emit_span("stage1-scheduling", state["submit"], cond.last_transition_time, uid)
            if svc := _detect_service_name(pod):
                service_by_uid[uid] = svc

    # Stage 2 + stage 5 start
    if "scheduled" in state and "running" not in state:
        for cs in pod.status.container_statuses or []:
            if cs.state and cs.state.running:
                state["running"] = cs.state.running.started_at
                emit_span("stage2-image-pull", state["scheduled"], state["running"], uid)
                # stage 5 end recorded via queue-proxy readiness
                break

    # Stage 3
    for cond in pod.status.conditions or []:
        if cond.type == "Ready" and cond.status == "True":
            if "ready" not in state:
                state["ready"] = cond.last_transition_time
                emit_span("stage3-container-init", state["running"], cond.last_transition_time, uid)

                if "stage5_thread" not in state:
                    t = threading.Thread(target=monitor_queue_proxy_ready,
                                         args=(ns, name, uid, scheduled_ts[uid]),
                                         daemon=True)
                    t.start()
                    state["stage5_thread"] = t

                ready_uid = uid
                ready_event.set()
            break

# ───────────────  K8s watch loop  ───────────────────────────
def setup_k8s():
    global v1_client
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    v1_client = client.CoreV1Api()

def watch_pods(ns, selector):
    setup_k8s()
    w = watch.Watch()
    for ev in w.stream(v1_client.list_namespaced_pod, namespace=ns,
                       label_selector=selector or "", timeout_seconds=120):
        process_pod(ev["object"])

# ─────────────  HTTP probing (4/6/7)  ───────────────────────
def probe(url, uid, ready_ts):
    # Stage 4
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

    # Stage 6
    emit_span("stage6-app-init", ready_ts, t4_end, uid)

    # Stage 7
    t7 = datetime.now(timezone.utc)
    try:
        status = requests.get(url, timeout=5).status_code
    except requests.RequestException:
        status = -1
    emit_span("stage7-first-request", t7, datetime.now(timezone.utc), uid)
    log.info("First request HTTP status: %s", status)

    print_profile_results(uid)

# ─────────────────────────── main ───────────────────────────
def main():
    pa = argparse.ArgumentParser("Knative cold-start profiler (K8s-only)")
    pa.add_argument("--namespace", default=os.getenv("NAMESPACE", "default"))
    pa.add_argument("--selector", default=os.getenv("SELECTOR", ""),
                    help="Label selector; blank = all pods")
    pa.add_argument("--url", required=True,
                    help="Route URL to probe (e.g. http://nginx.default)")
    args = pa.parse_args()

    threading.Thread(target=watch_pods, args=(args.namespace, args.selector), daemon=True).start()
    log.info("Profiler started – namespace=%s selector='%s'", args.namespace, args.selector or "<all>")

    while True:
        ready_event.wait()
        uid = ready_uid
        ready_event.clear()
        with pod_lock:
            if uid in processed:
                continue
            processed.add(uid)
            ready_ts = (pod_state[uid].get("ready")
                        or pod_state[uid].get("scheduled")
                        or datetime.now(timezone.utc))
        probe(args.url, uid, ready_ts)
        log.info("Completed pod %s – waiting for next …", uid[:6])

if __name__ == "__main__":
    main()
