#!/usr/bin/env python3
"""
Knative cold‑start stage profiler (loops forever, auto‑detects service name)
--------------------------------------------------------------------------
No need to pass --otel-service manually: when the first pod of a revision
appears the script reads its labels ("serving.knative.dev/service", fallback
"app"/"app.kubernetes.io/name") and uses that as the OTEL service name for
stage 5. If a span with that service.name never arrives, stage 5 is skipped but
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
pod_state = {}
pod_lock = threading.Lock()  # Lock for thread-safe access to pod_state
ready_event = threading.Event()
ready_uid: str | None = None
scheduled_ts = {}
service_by_uid = {}  # detected OTEL service.name per pod UID

# Track which spans we've already emitted per pod
emitted_spans = {}  # uid -> set of emitted span names
# Track which pods we've already fully processed
processed = set()

# ────────────────  Span emitter  ─────────────────────────────

def emit_span(name: str, start: datetime, end: datetime, uid: str):
    """Emit a span with deduplication"""
    # Initialize the set of emitted spans for this pod if needed
    if uid not in emitted_spans:
        emitted_spans[uid] = set()
    
    # Skip if we've already emitted this span
    if name in emitted_spans[uid]:
        return
    
    # Mark this span as emitted
    emitted_spans[uid].add(name)
    
    # Start and end the span
    span = tracer.start_span(name, start_time=int(_utc(start).timestamp() * 1e9))
    span.end(end_time=int(_utc(end).timestamp() * 1e9))
    
    # Calculate and log the duration
    duration = (end - start).total_seconds()
    log.info("%s [%s]: %s", name, uid[:6], _ms(duration))

# ──────────────  OTEL‑based runtime stage 5  ─────────────────

def wait_for_first_span(zipkin_base: str, service: str, start_ts: datetime, uid: str):
    """Try to find a span for the service and emit stage5 if found"""
    if not zipkin_base.startswith(('http://', 'https://')):
        zipkin_base = f"http://{zipkin_base}"
    
    api = f"{zipkin_base.rstrip('/')}/api/v2/traces?serviceName={quote_plus(service)}&limit=10&lookback=300s"
    log.info(f"Looking for spans from service '{service}' at {api}")
    
    retries = 120  # Try for ~1 minute
    for i in range(retries):
        try:
            r = requests.get(api, timeout=5)
            log.debug(f"Zipkin query attempt {i+1}/{retries}: status={r.status_code}, content_length={len(r.text)}")
            
            if r.ok and r.json():
                traces = r.json()
                span_count = sum(len(span) for trace in traces for span in trace)
                log.info(f"Found {span_count} spans for service '{service}'")
                emit_span("stage5-runtime-startup", start_ts, datetime.now(timezone.utc), uid)
                return True
        except requests.RequestException as e:
            log.warning(f"Zipkin request failed (attempt {i+1}/{retries}): {e}")
        except Exception as e:
            log.warning(f"Error processing Zipkin response (attempt {i+1}/{retries}): {e}")
        
        if i < retries - 1:  # Don't sleep on the last iteration
            time.sleep(0.5)
    
    log.warning(f"No span for service '{service}' after {retries} attempts; skipping stage5 for {uid[:6]}")
    return False

# ───────────────  Pod event processor  ───────────────────────

def _detect_service_name(pod) -> str | None:
    """Extract service name from pod labels"""
    lbl = pod.metadata.labels or {}
    return lbl.get("serving.knative.dev/service") or lbl.get("app.kubernetes.io/name") or lbl.get("app")


def process_pod(pod):
    """Process pod updates and emit spans for detected state changes"""
    global ready_uid
    uid = pod.metadata.uid
    
    with pod_lock:  # Thread-safe access to pod_state
        state = pod_state.setdefault(uid, {"emitted": set()})
        emitted = state["emitted"]  # Set of stages already emitted for this pod

        # Stage 1 - Scheduling
        state.setdefault("submit", pod.metadata.creation_timestamp)
        
        for c in pod.status.conditions or []:
            if c.type == "PodScheduled" and c.status == "True":
                state["scheduled"] = c.last_transition_time
                scheduled_ts[uid] = c.last_transition_time
                
                # Only emit stage1 once
                if "stage1" not in emitted:
                    emitted.add("stage1")
                    emit_span("stage1-scheduling", state["submit"], c.last_transition_time, uid)
                
                # Auto-detect service name once we schedule
                if uid not in service_by_uid:
                    if (svc := _detect_service_name(pod)):
                        service_by_uid[uid] = svc
                        log.info(f"Detected service.name '{svc}' for pod {uid[:6]}")

        # Stage 2 - Image Pull
        if "scheduled" in state:
            for cs in pod.status.container_statuses or []:
                if cs.state and cs.state.running:
                    state["running"] = cs.state.running.started_at
                    
                    # Only emit stage2 once
                    if "stage2" not in emitted and "scheduled" in state:
                        emitted.add("stage2")
                        emit_span("stage2-image-pull", state["scheduled"], state["running"], uid)
                    break

        # Stage 3 - Container Init & Ready
        for c in pod.status.conditions or []:
            if c.type == "Ready" and c.status == "True":
                state["ready"] = c.last_transition_time
                
                # Only emit stage3 once
                if "stage3" not in emitted and "running" in state:
                    emitted.add("stage3")
                    emit_span("stage3-container-init", state["running"], c.last_transition_time, uid)
                
                # Signal that this pod is ready for probing
                if uid not in processed and ready_uid != uid:
                    ready_uid = uid
                    ready_event.set()
                break

# ───────────────  Kubernetes watch loop  ────────────────────

def watch_pods(ns: str, selector: str):
    """Watch for pod events in the specified namespace with the given selector"""
    try:
        config.load_incluster_config()
        log.info("Using in-cluster Kubernetes configuration")
    except Exception:
        config.load_kube_config()
        log.info("Using local Kubernetes configuration")
    
    v1 = client.CoreV1Api()
    w = watch.Watch()
    
    while True:
        try:
            log.info(f"Watching pods in {ns} selector='{selector or '<all>'}'")
            for ev in w.stream(v1.list_namespaced_pod, namespace=ns, 
                               label_selector=selector, timeout_seconds=120):
                process_pod(ev["object"])
        except Exception as e:
            log.error(f"Error in pod watch loop: {e}")
            time.sleep(5)  # Wait before reconnecting

# ─────────────  HTTP probing (4,6,7)  ───────────────────────

def probe(url: str, uid: str, ready_ts: datetime):
    """Probe the URL to measure stages 4, 6, and 7"""
    # Stage 4 - Proxy Warmup
    t0 = datetime.now(timezone.utc)
    log.info(f"Starting to probe URL: {url}")
    
    max_attempts = 60  # ~30 seconds
    for attempt in range(max_attempts):
        try:
            response = requests.get(url, timeout=2)
            if response.status_code == 200:
                log.info(f"URL {url} is ready after {attempt+1} attempts")
                break
        except requests.RequestException as e:
            if attempt % 10 == 0:  # Log less frequently to reduce spam
                log.info(f"Probe attempt {attempt+1}/{max_attempts}: {e}")
        
        if attempt == max_attempts - 1:
            log.warning(f"URL {url} not ready after {max_attempts} attempts")
        time.sleep(0.5)
    
    t4_end = datetime.now(timezone.utc)
    emit_span("stage4-proxy-warmup", t0, t4_end, uid)

    # Stage 6 - App Init (from pod ready to first successful probe)
    emit_span("stage6-app-init", ready_ts, t4_end, uid)

    # Stage 7 - First Request
    t7 = datetime.now(timezone.utc)
    try:
        resp = requests.get(url, timeout=5)
        t7_end = datetime.now(timezone.utc)
        status = resp.status_code
    except requests.RequestException as e:
        t7_end = datetime.now(timezone.utc)
        log.warning(f"Error in stage7 request: {e}")
        status = -1
    
    emit_span("stage7-first-request", t7, t7_end, uid)
    log.info(f"stage7-first-request [{uid[:6]}]: {_ms((t7_end-t7).total_seconds())} (HTTP {status})")

# ────────────────────────────  main  ─────────────────────────

def main() -> None:
    p = argparse.ArgumentParser("Knative cold‑start profiler – auto service detection")
    p.add_argument("--namespace", default=os.getenv("NAMESPACE", "default"))
    p.add_argument("--selector", default=os.getenv("SELECTOR", ""), help="Label selector; blank = all pods")
    p.add_argument("--url", required=True, help="Route URL to probe (e.g. http://nginx.default)")
    p.add_argument("--zipkin", default=os.getenv("ZIPKIN_API", "http://jaeger-zipkin.observability.svc.cluster.local:9411"))
    p.add_argument("--otel-service", default=os.getenv("SERVICE_NAME", ""), help="Override detected service name")
    p.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = p.parse_args()

    if args.debug:
        logging.getLogger("stage-profiler").setLevel(logging.DEBUG)

    # Start the pod watcher in a separate thread
    watcher_thread = threading.Thread(
        target=watch_pods, 
        args=(args.namespace, args.selector), 
        daemon=True
    )
    watcher_thread.start()

    log.info(f"Profiler started – waiting for pods in {args.namespace} with selector '{args.selector or '<all>'}'")
    
    while True:
        # Wait for a pod to be ready
        ready_event.wait()
        uid = ready_uid
        ready_event.clear()
        
        with pod_lock:
            # Skip if we've already processed this pod
            if uid in processed:
                continue
            
            # Mark this pod as processed
            processed.add(uid)
            
            # Get the service name (prefer command line arg, then detected)
            svc_name = args.otel_service or service_by_uid.get(uid)
            
            # Get the ready timestamp for this pod
            pod_ready_ts = pod_state[uid].get("ready", pod_state[uid].get("scheduled", datetime.now(timezone.utc)))

        # Start stage5 check in a separate thread if we have a service name
        if svc_name and uid in scheduled_ts:
            log.info(f"Starting stage5 check for service '{svc_name}', pod {uid[:6]}")
            threading.Thread(
                target=wait_for_first_span,
                args=(args.zipkin, svc_name, scheduled_ts[uid], uid),
                daemon=True
            ).start()
        else:
            log.warning(f"No service name detected for pod {uid[:6]} – skipping stage5")

        # Probe the URL for stages 4, 6, and 7
        probe(args.url, uid, pod_ready_ts)
        log.info(f"Completed pod {uid[:6]} – awaiting next revision...")


if __name__ == "__main__":
    main()
