#!/usr/bin/env python3
"""
Knative cold‑start stage profiler (loops forever, auto‑detects service name)
--------------------------------------------------------------------------
Using Kubernetes API to detect all stages including stage 5 (runtime startup).
Prints all stages in chronological order after collecting complete timing data.
"""

import os
import logging
import argparse
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlparse
from collections import OrderedDict

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
service_by_uid = {}  # detected OTEL service.name per pod UID

# Track which pods we've already processed
processed = set()

# Kubernetes API client
v1_client = None  # Core API client

# Stage timing collector
pod_timings = {}  # uid -> {stage_name: (start_time, end_time)}
timing_complete = {}  # uid -> bool (whether all timings are collected)

# Stage names in chronological order
STAGE_ORDER = [
    "stage1-scheduling",
    "stage2-image-pull", 
    "stage3-container-init",
    "stage5-runtime-startup",
    "stage4-proxy-warmup",
    "stage6-app-init",
    "stage7-first-request"
]

# ────────────────  Span collection and emission  ─────────────

def record_timing(uid: str, stage: str, start: datetime, end: datetime):
    """Record stage timing for later ordered emission"""
    with pod_lock:
        if uid not in pod_timings:
            pod_timings[uid] = {}
        
        # Store timing data
        pod_timings[uid][stage] = (start, end)
        log.debug(f"Recorded timing for {stage} [pod {uid[:6]}]: {_ms((end-start).total_seconds())}")

def emit_all_spans(uid: str):
    """Emit all spans for a pod in chronological order"""
    with pod_lock:
        if uid not in pod_timings:
            log.warning(f"No timings found for pod {uid[:6]}")
            return
        
        timings = pod_timings[uid]
        
        # Check if we have enough stages to emit
        # We need at least stages 1-4 and 6-7 (can skip 5 if needed)
        required_stages = [s for s in STAGE_ORDER if s != "stage5-runtime-startup"]
        missing = [s for s in required_stages if s not in timings]
        if missing:
            log.warning(f"Missing required stages for pod {uid[:6]}: {missing}")
            return
        
        # Mark this pod as complete
        timing_complete[uid] = True
        
        # Get service name for this pod
        service = service_by_uid.get(uid, "unknown")
        
        # Print header
        log.info(f"=== Complete profile for pod {uid[:6]} (service: {service}) ===")
        
        # Emit spans in chronological order
        for stage in STAGE_ORDER:
            if stage in timings:
                start, end = timings[stage]
                # Emit span to OpenTelemetry
                span = tracer.start_span(stage, start_time=int(_utc(start).timestamp() * 1e9))
                span.end(end_time=int(_utc(end).timestamp() * 1e9))
                
                # Log with consistent formatting
                duration = (end - start).total_seconds()
                log.info(f"{stage:<25} : {_ms(duration)}")
        
        # Print footer
        log.info(f"=== End of profile for pod {uid[:6]} ===")

# ──────────────  Stage 5 using K8s API  ──────────────────────

def monitor_queue_proxy_ready(namespace, pod_name, uid, scheduled_time):
    """
    Monitor when queue-proxy container becomes ready, indicating runtime startup
    """
    global v1_client
    
    log.debug(f"Monitoring queue-proxy readiness for pod {pod_name}")
    start_time = scheduled_time
    
    # Poll for queue-proxy container status
    for _ in range(120):  # Try for ~2 minutes
        try:
            pod = v1_client.read_namespaced_pod(pod_name, namespace)
            
            # Check container statuses
            if pod.status.container_statuses:
                for container in pod.status.container_statuses:
                    if container.name == "queue-proxy" and container.ready:
                        # Queue proxy is ready!
                        end_time = datetime.now(timezone.utc)
                        log.debug(f"Queue proxy is ready in pod {pod_name}")
                        
                        # Record timing data
                        record_timing(uid, "stage5-runtime-startup", start_time, end_time)
                        
                        # Check if we can finalize this pod now
                        maybe_finalize_pod(uid)
                        return True
        except Exception as e:
            log.warning(f"Error checking queue-proxy status: {e}")
        
        time.sleep(1)
    
    log.warning(f"Queue proxy never became ready for pod {pod_name}")
    # Even without stage 5, we can still finalize if other stages are complete
    maybe_finalize_pod(uid)
    return False

# ───────────────  Pod event processor  ───────────────────────

def _detect_service_name(pod) -> str | None:
    """Extract service name from pod labels"""
    lbl = pod.metadata.labels or {}
    return lbl.get("serving.knative.dev/service") or lbl.get("app.kubernetes.io/name") or lbl.get("app")

def maybe_finalize_pod(uid: str):
    """Check if we have all needed stages and can finalize the pod"""
    with pod_lock:
        # Skip if already complete or processed
        if uid in timing_complete or uid in processed:
            return
        
        # Check if we have all required timings
        timings = pod_timings.get(uid, {})
        
        # We need at least stages 1-4 and 6-7 (can skip 5 if needed)
        required_stages = [s for s in STAGE_ORDER if s != "stage5-runtime-startup"]
        
        if all(stage in timings for stage in required_stages):
            emit_all_spans(uid)
            processed.add(uid)

def process_pod(pod):
    """Process pod updates and record stage timings"""
    global ready_uid
    uid = pod.metadata.uid
    pod_name = pod.metadata.name
    namespace = pod.metadata.namespace
    
    # Skip if already processed
    if uid in processed:
        return
    
    with pod_lock:
        state = pod_state.setdefault(uid, {})
        
        # Stage 1 - Scheduling
        state.setdefault("submit", pod.metadata.creation_timestamp)
        
        for c in pod.status.conditions or []:
            if c.type == "PodScheduled" and c.status == "True":
                if "scheduled" not in state:
                    state["scheduled"] = c.last_transition_time
                    record_timing(uid, "stage1-scheduling", state["submit"], c.last_transition_time)
                
                # Auto-detect service name once we schedule
                if uid not in service_by_uid:
                    if (svc := _detect_service_name(pod)):
                        service_by_uid[uid] = svc
                        log.debug(f"Detected service.name '{svc}' for pod {uid[:6]}")

        # Stage 2 - Image Pull (ORIGINAL LOGIC) - Check ANY container that's running
        if "scheduled" in state and "running" not in state:
            for cs in pod.status.container_statuses or []:
                if cs.state and cs.state.running:
                    state["running"] = cs.state.running.started_at
                    record_timing(uid, "stage2-image-pull", state["scheduled"], state["running"])
                    break

        # Stage 3 - Container Init & Ready (ORIGINAL LOGIC)
        for c in pod.status.conditions or []:
            if c.type == "Ready" and c.status == "True":
                if "ready" not in state:
                    state["ready"] = c.last_transition_time
                    
                    # Original logic for stage3
                    if "running" in state:
                        record_timing(uid, "stage3-container-init", state["running"], c.last_transition_time)
                    
                    # Start stage5 monitoring in a background thread if not already started
                    if "stage5_started" not in state and "scheduled" in state:
                        threading.Thread(
                            target=monitor_queue_proxy_ready,
                            args=(namespace, pod_name, uid, state["scheduled"]),
                            daemon=True
                        ).start()
                        state["stage5_started"] = True
                    
                    # Signal that this pod is ready for probing
                    ready_uid = uid
                    ready_event.set()
                break

# ───────────────  Kubernetes watch loop  ────────────────────

def setup_kubernetes_clients():
    """Set up Kubernetes API clients"""
    global v1_client
    
    try:
        config.load_incluster_config()
        log.info("Using in-cluster Kubernetes configuration")
    except Exception:
        config.load_kube_config()
        log.info("Using local Kubernetes configuration")
    
    v1_client = client.CoreV1Api()

def watch_pods(ns: str, selector: str):
    """Watch for pod events in the specified namespace with the given selector"""
    global v1_client
    
    setup_kubernetes_clients()
    w = watch.Watch()
    
    while True:
        try:
            log.info(f"Watching pods in {ns} selector='{selector or '<all>'}'")
            for ev in w.stream(v1_client.list_namespaced_pod, namespace=ns, 
                               label_selector=selector, timeout_seconds=120):
                process_pod(ev["object"])
        except Exception as e:
            log.error(f"Error in pod watch loop: {e}")
            time.sleep(5)  # Wait before reconnecting

# ─────────────  HTTP probing (4,6,7)  ───────────────────────

def probe(url: str, uid: str):
    """Probe the URL to measure stages 4, 6, and 7"""
    with pod_lock:
        if uid not in pod_state or "ready" not in pod_state[uid]:
            log.warning(f"Cannot probe pod {uid[:6]} - not ready")
            return
        
        ready_ts = pod_state[uid]["ready"]
    
    # Stage 4 - Proxy Warmup
    t0 = datetime.now(timezone.utc)
    log.debug(f"Starting to probe URL: {url}")
    
    max_attempts = 60  # ~30 seconds
    for attempt in range(max_attempts):
        try:
            response = requests.get(url, timeout=2)
            if response.status_code == 200:
                log.debug(f"URL {url} is ready after {attempt+1} attempts")
                break
        except requests.RequestException as e:
            if attempt % 10 == 0:  # Log less frequently to reduce spam
                log.debug(f"Probe attempt {attempt+1}/{max_attempts}: {e}")
        
        if attempt == max_attempts - 1:
            log.warning(f"URL {url} not ready after {max_attempts} attempts")
        time.sleep(0.5)
    
    t4_end = datetime.now(timezone.utc)
    record_timing(uid, "stage4-proxy-warmup", t0, t4_end)

    # Stage 6 - App Init (from pod ready to first successful probe)
    record_timing(uid, "stage6-app-init", ready_ts, t4_end)

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
    
    record_timing(uid, "stage7-first-request", t7, t7_end)
    log.debug(f"Recorded stage7 for [{uid[:6]}]: {_ms((t7_end-t7).total_seconds())} (HTTP {status})")
    
    # Now we should have all stages except possibly stage5
    maybe_finalize_pod(uid)

# ────────────────────────────  main  ─────────────────────────

def main() -> None:
    p = argparse.ArgumentParser("Knative cold‑start profiler – Ordered output")
    p.add_argument("--namespace", default=os.getenv("NAMESPACE", "default"))
    p.add_argument("--selector", default=os.getenv("SELECTOR", ""), help="Label selector; blank = all pods")
    p.add_argument("--url", required=True, help="Route URL to probe (e.g. http://nginx.default)")
    p.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = p.parse_args()

    if args.debug:
        logging.getLogger("stage-profiler").setLevel(logging.DEBUG)

    # Set up Kubernetes API clients
    setup_kubernetes_clients()

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
        
        # Skip if we've already processed this pod
        if uid in processed:
            continue
        
        # Probe the URL for stages 4, 6, and 7
        probe(args.url, uid)
        
        # Wait for the next pod event
        log.debug(f"Waiting for next pod event...")


if __name__ == "__main__":
    main()
