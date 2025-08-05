#!/usr/bin/env python3
"""
Knative cold‑start stage profiler (loops forever)
------------------------------------------------
Profiles every new Knative revision that appears, then waits for the next one.
Stages
  1. scheduling               (PodSubmitted ➜ PodScheduled)
  2. image pull / unpack       (PodScheduled ➜ first container Running)
  3. container init            (Running ➜ Ready)
  4. queue‑proxy warm‑up       (URL probe until first 200)
  5. runtime start‑up          (PodScheduled ➜ first OTEL span)
  6. app‑level init            (Ready ➜ first 200)
  7. first‑request latency

The script never exits: after finishing one revision it resets its state and
waits for the next Pod that matches the selector to reach **Ready**.
"""

import os
import logging
import argparse
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, quote_plus
from collections import defaultdict

import requests
from kubernetes import client, config, watch
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# ─────────────────────────  Logging  ──────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("stage-profiler")

# ───────────────────  Helper functions  ──────────────────────

def _strip_suffix(txt: str, suf: str) -> str:
    return txt[:-len(suf)] if txt.endswith(suf) else txt


def _sanitize_endpoint(raw: str) -> str:
    """Remove scheme & OTLP sub‑paths → '<host>:<port>'"""
    raw = _strip_suffix(_strip_suffix(raw.strip(), "/v1/traces"), "/v1/metrics")
    if raw.startswith(("http://", "https://")):
        raw = urlparse(raw).netloc
    return raw or "localhost:4317"


def _utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _ms(delta_sec: float) -> str:
    return f"{delta_sec * 1e3:.2f} ms"

# ───────────────────  OpenTelemetry setup  ───────────────────
endpoint_env = (
    os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or
    os.getenv("OTEL_COLLECTOR_ENDPOINT") or
    "localhost:4317"
)
collector_ep = _sanitize_endpoint(endpoint_env)

provider = TracerProvider(resource=Resource({"service.name": "stage-profiler"}))
provider.add_span_processor(BatchSpanProcessor(
    OTLPSpanExporter(endpoint=collector_ep, insecure=True)
))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

# ───────────────  Global mutable state  ──────────────────────

pod_state: dict[str, dict] = {}
ready_event  = threading.Event()
ready_uid: str | None = None
scheduled_ts: dict[str, datetime] = {}

# Dedup guards
seen: set[tuple[str, str]] = set()          # (uid, stage)
processed: set[str] = set()                 # pods already fully profiled

# ────────────────  Span emitter  ─────────────────────────────

def emit_span(name: str, start: datetime, end: datetime, uid: str):
    key = (uid, name)
    if key in seen:
        return
    seen.add(key)

    start, end = map(_utc, (start, end))
    span = tracer.start_span(name, start_time=int(start.timestamp() * 1e9))
    span.end(end_time=int(end.timestamp() * 1e9))
    log.info("%s [%s]: %s", name, uid[:6], _ms((end - start).total_seconds()))

# ───────────────  OTEL‑based runtime startup  ────────────────

def wait_for_first_span(zipkin_base: str, otel_service: str, start_ts: datetime, uid: str):
    api = f"{zipkin_base.rstrip('/')}/api/v2/traces?serviceName={quote_plus(otel_service)}&limit=1&lookback=45s"
    while True:
        try:
            r = requests.get(api, timeout=2)
            if r.ok and r.json():
                emit_span("stage5-runtime-startup", start_ts, datetime.now(timezone.utc), uid)
                return
        except requests.RequestException:
            pass
        time.sleep(0.5)

# ───────────────  Pod event processor  ───────────────────────

def process_pod(pod):
    global ready_uid

    uid = pod.metadata.uid
    state = pod_state.setdefault(uid, {})

    # Stage 1 – scheduling
    state.setdefault("submit", pod.metadata.creation_timestamp)
    for cond in pod.status.conditions or []:
        if cond.type == "PodScheduled" and cond.status == "True" and "scheduled" not in state:
            state["scheduled"] = cond.last_transition_time
            scheduled_ts[uid] = cond.last_transition_time
            emit_span("stage1-scheduling", state["submit"], cond.last_transition_time, uid)

    # Stage 2 – image pull / unpack
    if "scheduled" in state and "running" not in state:
        for cs in pod.status.container_statuses or []:
            if cs.state and cs.state.running:
                state["running"] = cs.state.running.started_at
                emit_span("stage2-image-pull", state["scheduled"], state["running"], uid)
                break

    # Stage 3 – container init / Ready
    for cond in pod.status.conditions or []:
        if cond.type == "Ready" and cond.status == "True":
            if "running" in state:
                emit_span("stage3-container-init", state["running"], cond.last_transition_time, uid)
            if uid not in processed and ready_uid != uid:
                ready_uid = uid
                ready_event.set()          # signal main loop
            break

# ───────────────  Kubernetes watch loop  ────────────────────

def watch_pods(namespace: str, selector: str):
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()

    v1 = client.CoreV1Api()
    w = watch.Watch()
    log.info("Watching pods in %s selector='%s'", namespace, selector)

    for event in w.stream(v1.list_namespaced_pod,
                          namespace=namespace,
                          label_selector=selector,
                          timeout_seconds=0):
        process_pod(event["object"])

# ─────────────  HTTP probing (stages 4, 6, 7)  ───────────────

def probe(url: str, uid: str, ready_ts: datetime):
    # Stage 4 – proxy warm‑up
    t0 = datetime.now(timezone.utc)
    span4 = tracer.start_span("stage4-proxy-warmup", start_time=int(t0.timestamp() * 1e9))
    while True:
        try:
            if requests.get(url, timeout=0.5).status_code == 200:
                break
        except requests.RequestException:
            time.sleep(0.1)
    span4.end()
    emit_span("stage4-proxy-warmup", t0, datetime.now(timezone.utc), uid)

    # Stage 6 – app‑level init
    emit_span("stage6-app-init", ready_ts, datetime.now(timezone.utc), uid)

    # Stage 7 – first request latency
    t7 = datetime.now(timezone.utc)
    span7 = tracer.start_span("stage7-first-request", start_time=int(t7.timestamp() * 1e9))
    resp = requests.get(url)
    span7.end()
    log.info("stage7-first-request [%s]: %s (HTTP %d)", uid[:6], _ms((datetime.now(timezone.utc) - t7).total_seconds()), resp.status_code)

# ────────────────────────────  main  ─────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Knative cold‑start stage profiler – continuous mode")
    parser.add_argument("--namespace", default=os.getenv("NAMESPACE", "default"))
    parser.add_argument("--selector",  default=os.getenv("SELECTOR", ""))
    parser.add_argument("--url",       required=True,
                        help="Full URL of the Route, e.g. http://nginx.default.example.com")
    parser.add_argument("--otel-service", default=os.getenv("SERVICE_NAME", ""),
                        help="service.name label emitted by the workload")
    parser.add_argument("--zipkin", default=os.getenv("ZIPKIN_API", "http://jaeger-zipkin.observability.svc.cluster.local:9411"),
                        help="Base URL of Zipkin v2 API exposed by the collector")
    args = parser.parse_args()

    if not args.otel_service:
        log.error("Need --otel-service or $SERVICE_NAME to detect first span")
        return

    if not args.selector:
        args.selector = f"serving.knative.dev/service={args.otel_service}"

    # Spawn pod watcher thread
    threading.Thread(target=watch_pods,
                     args=(args.namespace, args.selector),
                     daemon=True).start()

    log.info("Started – waiting for successive Pods to become Ready …")

    while True:
        ready_event.wait()    # block until a new pod hits Ready
        uid = ready_uid
        ready_event.clear()
        if uid in processed:
            continue          # already handled this one (duplicate event)
        processed.add(uid)

        # spawn span‑watcher
        if uid in scheduled_ts:
            threading.Thread(target=wait_for_first_span,
                             args=(args.zipkin, args.otel_service, scheduled_ts[uid], uid),
                             daemon=True).start()

        # run HTTP probe pipeline
        probe(args.url, uid, pod_state[uid].get("scheduled", datetime.now(timezone.utc)))

        log.info("Completed profiling of pod %s – waiting for next revision …", uid[:6])


if __name__ == "__main__":
    main()
