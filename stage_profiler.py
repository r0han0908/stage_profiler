#!/usr/bin/env python3
"""
Knative cold‑start stage profiler (loops forever, auto‑detects service name)
--------------------------------------------------------------------------
No need to pass --otel-service manually: when the first pod of a revision
appears the script reads its labels ("serving.knative.dev/service", fallback
"app"/"app.kubernetes.io/name") and uses that as the OTEL service name for
stage 5. If a span with that service.name never arrives, stage 5 is skipped but
other stages are still reported.
"""

import os
import logging
import argparse
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, quote_plus

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
    raw = _strip_suffix(_strip_suffix(raw.strip(), "/v1/traces"), "/v1/metrics")
    if raw.startswith(("http://", "https://")):
        raw = urlparse(raw).netloc
    return raw or "localhost:4317"


def _utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _ms(sec: float) -> str:
    return f"{sec * 1e3:.2f} ms"

# ───────────────────  OpenTelemetry setup  ───────────────────
collector_ep = _sanitize_endpoint(os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or
                                 os.getenv("OTEL_COLLECTOR_ENDPOINT") or
                                 "localhost:4317")
provider = TracerProvider(resource=Resource({"service.name": "stage-profiler"}))
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=collector_ep, insecure=True)))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

# ───────────────  Global mutable state  ──────────────────────
pod_state          = {}
ready_event        = threading.Event()
ready_uid: str | None = None
scheduled_ts       = {}
service_by_uid     = {}  # detected OTEL service.name per pod UID

seen       : set[tuple[str, str]] = set()
processed  : set[str]             = set()

# ────────────────  Span emitter  ─────────────────────────────

def emit_span(name: str, start: datetime, end: datetime, uid: str):
    key = (uid, name)
    if key in seen:
        return
    seen.add(key)
    span = tracer.start_span(name, start_time=int(_utc(start).timestamp() * 1e9))
    span.end(end_time=int(_utc(end).timestamp() * 1e9))
    log.info("%s [%s]: %s", name, uid[:6], _ms((end - start).total_seconds()))

# ──────────────  OTEL‑based runtime stage 5  ─────────────────

def wait_for_first_span(zipkin_base: str, service: str, start_ts: datetime, uid: str):
    api = f"{zipkin_base.rstrip('/')}/api/v2/traces?serviceName={quote_plus(service)}&limit=1&lookback=60s"
    for _ in range(120):  # poll ~1 min then give up
        try:
            r = requests.get(api, timeout=2)
            if r.ok and r.json():
                emit_span("stage5-runtime-startup", start_ts, datetime.now(timezone.utc), uid)
                return
        except requests.RequestException:
            pass
        time.sleep(0.5)
    log.warning("No span for service '%s' within window; skipping stage5 for %s", service, uid[:6])

# ───────────────  Pod event processor  ───────────────────────

def _detect_service_name(pod) -> str | None:
    lbl = pod.metadata.labels or {}
    return lbl.get("serving.knative.dev/service") or lbl.get("app.kubernetes.io/name") or lbl.get("app")


def process_pod(pod):
    global ready_uid
    uid = pod.metadata.uid
    state = pod_state.setdefault(uid, {})

    # Stage 1
    state.setdefault("submit", pod.metadata.creation_timestamp)
    for c in pod.status.conditions or []:
        if c.type == "PodScheduled" and c.status == "True" and "scheduled" not in state:
            state["scheduled"] = c.last_transition_time
            scheduled_ts[uid] = c.last_transition_time
            emit_span("stage1-scheduling", state["submit"], c.last_transition_time, uid)
            # autodetect service name once we schedule
            if uid not in service_by_uid:
                if (svc := _detect_service_name(pod)):
                    service_by_uid[uid] = svc
                    log.info("Detected service.name '%s' for pod %s", svc, uid[:6])

    # Stage 2
    if "scheduled" in state and "running" not in state:
        for cs in pod.status.container_statuses or []:
            if cs.state and cs.state.running:
                state["running"] = cs.state.running.started_at
                emit_span("stage2-image-pull", state["scheduled"], state["running"], uid)
                break

    # Stage 3 / Ready
    for c in pod.status.conditions or []:
        if c.type == "Ready" and c.status == "True":
            if "running" in state:
                emit_span("stage3-container-init", state["running"], c.last_transition_time, uid)
            if uid not in processed and ready_uid != uid:
                ready_uid = uid
                ready_event.set()
            break

# ───────────────  Kubernetes watch loop  ────────────────────

def watch_pods(ns: str, selector: str):
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    v1 = client.CoreV1Api()
    w  = watch.Watch()
    log.info("Watching pods in %s selector='%s'", ns, selector or "<all>")
    for ev in w.stream(v1.list_namespaced_pod, namespace=ns, label_selector=selector, timeout_seconds=0):
        process_pod(ev["object"])

# ─────────────  HTTP probing (4,6,7)  ───────────────────────

def probe(url: str, uid: str, ready_ts: datetime):
    t0 = datetime.now(timezone.utc)
    while True:
        try:
            if requests.get(url, timeout=0.5).status_code == 200:
                break
        except requests.RequestException:
            time.sleep(0.1)
    emit_span("stage4-proxy-warmup", t0, datetime.now(timezone.utc), uid)

    emit_span("stage6-app-init", ready_ts, datetime.now(timezone.utc), uid)

    t7 = datetime.now(timezone.utc)
    resp = requests.get(url)
    emit_span("stage7-first-request", t7, datetime.now(timezone.utc), uid)
    log.info("stage7-first-request [%s]: %s (HTTP %d)", uid[:6], _ms((datetime.now(timezone.utc)-t7).total_seconds()), resp.status_code)

# ────────────────────────────  main  ─────────────────────────

def main() -> None:
    p = argparse.ArgumentParser("Knative cold‑start profiler – auto service detection")
    p.add_argument("--namespace", default=os.getenv("NAMESPACE", "default"))
    p.add_argument("--selector",  default=os.getenv("SELECTOR", ""), help="Label selector; blank = all pods")
    p.add_argument("--url",       required=True, help="Route URL to probe (e.g. http://nginx.default)")
    p.add_argument("--zipkin",    default=os.getenv("ZIPKIN_API", "http://jaeger-zipkin.observability.svc.cluster.local:9411"))
    args = p.parse_args()

    threading.Thread(target=watch_pods, args=(args.namespace, args.selector), daemon=True).start()

    log.info("Profiler started – waiting for pods …")
    while True:
        ready_event.wait()
        uid = ready_uid
        ready_event.clear()
        if uid in processed:
            continue
        processed.add(uid)

        svc_name = service_by_uid.get(uid)
        if svc_name and uid in scheduled_ts:
            threading.Thread(target=wait_for_first_span, args=(args.zipkin, svc_name, scheduled_ts[uid], uid), daemon=True).start()
        else:
            log.info("No service name for pod %s – skipping stage5", uid[:6])

        probe(args.url, uid, pod_state[uid].get("scheduled", datetime.now(timezone.utc)))
        log.info("Completed pod %s – awaiting next revision …", uid[:6])


if __name__ == "__main__":
    main()
