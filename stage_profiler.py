#!/usr/bin/env python3
"""
Knative cold‑start stage profiler (loops forever, auto‑detects service name)
--------------------------------------------------------------------------
Using Kubernetes API to detect all stages including stage 5 (runtime startup).
Results are printed in a clean, formatted table.
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
scheduled_ts = {}
service_by_uid = {}  # detected OTEL service.name per pod UID

# Track which pods we've already processed
processed = set()

# Track timing data for pretty printing
pod_timings = {}  # uid -> {stage_name: (start_time, end_time, duration_ms)}

# Stage names and descriptions for pretty printing
STAGES = OrderedDict([
    ("stage1-scheduling", "Pod Scheduling"),
    ("stage2-image-pull", "Image Pull"),
    ("stage3-container-init", "Container Init"),
    ("stage5-runtime-startup", "Runtime Startup"),
    ("stage4-proxy-warmup", "Proxy Warmup"),
    ("stage6-app-init", "App Init"),
    ("stage7-first-request", "First Request")
])

# Kubernetes API client
v1_client = None

# ────────────────  Span emitter  ─────────────────────────────

def emit_span(name: str, start: datetime, end: datetime, uid: str):
    """Emit a span and record timing data"""
    # Ensure we have reasonable timestamps
    if end <= start:
        log.warning(f"Invalid timing for {name} [pod {uid[:6]}]: start={start}, end={end}, using 1ms")
        end = start.replace(microsecond=start.microsecond + 1000)
    
    # Start and end the span
    span = tracer.start_span(name, start_time=int(_utc(start).timestamp() * 1e9))
    span.end(end_time=int(_utc(end).timestamp() * 1e9))
    
    # Calculate and record duration
    duration = (end - start).total_seconds()
    
    # Store timing data for later printing
    with pod_lock:
        if uid not in pod_timings:
            pod_timings[uid] = {}
        pod_timings[uid][name] = (start, end, duration * 1000)  # Store ms
    
    # Log immediately too
    log.info("%s [%s]: %s", STAGES.get(name, name), uid[:6], _ms(duration))

def print_profile_results(uid: str):
    """Print profile results in a nicely formatted table"""
    with pod_lock:
        if uid not in pod_timings:
            log.warning(f"No timing data available for pod {uid[:6]}")
            return
            
        timings = pod_timings[uid]
        service = service_by_uid.get(uid, "unknown")
        
        # Calculate total coldstart time
        total_ms = 0
        if "stage1-scheduling" in timings and "stage7-first-request" in timings:
            start = timings["stage1-scheduling"][0]  # First stage start
            end = timings["stage7-first-request"][1]  # Last stage end
            total_ms = (end - start).total_seconds() * 1000
        
        # Print header
        print("\n")
        print("┏" + "━" * 70 + "┓")
        print(f"┃ {'COLD START PROFILE':^68} ┃")
        print(f"┃ {'Pod: ' + uid[:8] + '  Service: ' + service:^68} ┃")
        print("┣" + "━" * 70 + "┫")
        print(f"┃ {'Stage':^30} │ {'Duration':^18} │ {'% of Total':^15} ┃")
        print("┠" + "─" * 30 + "┼" + "─" * 18 + "┼" + "─" * 15 + "┨")
        
        # Print each stage in order
        cumulative_ms = 0
        
        for stage_id, stage_name in STAGES.items():
            if stage_id in timings:
                _, _, duration_ms = timings[stage_id]
                cumulative_ms += duration_ms
                
                # Calculate percentage of total time
                percentage = (duration_ms / total_ms * 100) if total_ms > 0 else 0
                
                # Print formatted row
                print(f"┃ {stage_name:30} │ {_ms(duration_ms/1000):18} │ {percentage:13.1f}% ┃")
        
        # Print footer with total
        print("┠" + "─" * 30 + "┼" + "─" * 18 + "┼" + "─" * 15 + "┨")
        print(f"┃ {'TOTAL COLD START TIME':30} │ {_ms(total_ms/1000):18} │ {100:13.1f}% ┃")
        print("┗" + "━" * 70 + "┛")
        print("\n")

# ──────────────  Stage 5 using K8s API  ──────────────────────

def monitor_queue_proxy_ready(namespace, pod_name, uid, scheduled_time):
    """Monitor when queue-proxy container becomes ready, indicating runtime startup"""
    global v1_client
    
    log.info(f"Monitoring queue-proxy readiness for pod {pod_name}")
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
                        log.info(f"Queue proxy is ready in pod {pod_name}")
                        
                        # Record timestamp and emit span
                        emit_span("stage5-runtime-startup", start_time, end_time, uid)
                        return True
        except Exception as e:
            log.warning(f"Error checking queue-proxy status: {e}")
        
        time.sleep(1)
    
    log.warning(f"Queue proxy never became ready for pod {pod_name}, skipping stage5")
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
    pod_name = pod.metadata.name
    namespace = pod.metadata.namespace
    
    # Skip if already fully processed
    if uid in processed:
        return
    
    with pod_lock:  # Thread-safe access to pod_state
        state = pod_state.setdefault(uid, {})

        # Stage 1 - Scheduling
        state.setdefault("submit", pod.metadata.creation_timestamp)
        
        for c in pod.status.conditions or []:
            if c.type == "PodScheduled" and c.status == "True" and "scheduled" not in state:
                state["scheduled"] = c.last_transition_time
                scheduled_ts[uid] = c.last_transition_time
                emit_span("stage1-scheduling", state["submit"], c.last_transition_time, uid)
                
                # Auto-detect service name once we schedule
                if uid not in service_by_uid:
                    if (svc := _detect_service_name(pod)):
                        service_by_uid[uid] = svc
                        log.info(f"Detected service.name '{svc}' for pod {uid[:6]}")

        # Stage 2 - Image Pull (ORIGINAL LOGIC) - any container running
        if "scheduled" in state and "running" not in state:
            for cs in pod.status.container_statuses or []:
                if cs.state and cs.state.running:
                    state["running"] = cs.state.running.started_at
                    emit_span("stage2-image-pull", state["scheduled"], state["running"], uid)
                    break

        # Stage 3 - Container Init & Ready (ORIGINAL LOGIC)
        for c in pod.status.conditions or []:
            if c.type == "Ready" and c.status == "True":
                if "ready" not in state:
                    state["ready"] = c.last_transition_time
                    
                    # Original logic for stage3
                    if "running" in state:
                        emit_span("stage3-container-init", state["running"], c.last_transition_time, uid)
                    
                    # Start stage5 monitoring in a background thread
                    if "stage5_thread" not in state and "scheduled" in state:
                        thread = threading.Thread(
                            target=monitor_queue_proxy_ready,
                            args=(namespace, pod_name, uid, scheduled_ts[uid]),
                            daemon=True
                        )
                        thread.start()
                        state["stage5_thread"] = thread
                    
                    # Signal that this pod is ready for probing
                    if uid not in processed and ready_uid != uid:
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
    
    # First check existing pods
    try:
        existing_pods = v1_client.list_namespaced_pod(namespace=ns, label_selector=selector)
        if existing_pods.items:
            log.info(f"Found {len(existing_pods.items)} existing pods")
            for pod in existing_pods.items:
                log.info(f"Processing existing pod: {pod.metadata.name}")
                process_pod(pod)
    except Exception as e:
        log.error(f"Error checking existing pods: {e}")
    
    # Now watch for new pods
    while True:
        try:
            log.info(f"Watching pods in {ns} selector='{selector or '<all>'}'")
            for ev in w.stream(v1_client.list_namespaced_pod, namespace=ns, 
                               label_selector=selector, timeout_seconds=120):
                pod = ev["object"]
                log.info(f"Pod event: {ev['type']} - {pod.metadata.name}")
                process_pod(pod)
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
    log.info(f"First request HTTP status: {status}")
    
    # Print the profile results
    print_profile_results(uid)

# ────────────────────────────  main  ─────────────────────────

def main() -> None:
    p = argparse.ArgumentParser("Knative cold‑start profiler – K8s API based monitoring")
    p.add_argument("--namespace", default=os.getenv("NAMESPACE", "default"))
    p.add_argument("--selector", default=os.getenv("SELECTOR", ""), help="Label selector; blank = all pods")
    p.add_argument("--url", required=True, help="Route URL to probe (e.g. http://nginx.default)")
    p.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = p.parse_args()

    if args.debug:
        logging.getLogger("stage-profiler").setLevel(logging.DEBUG)

    # Set up Kubernetes API clients
    setup_kubernetes_clients()

    # Print header
    print("\n" + "=" * 72)
    print(f"{'KNATIVE COLD START PROFILER':^72}")
    print(f"{'Namespace: ' + args.namespace + '  Selector: ' + (args.selector or '<all>')}".center(72))
    print("=" * 72 + "\n")

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
            
            # Get the ready timestamp for this pod
            pod_ready_ts = pod_state[uid].get("ready", pod_state[uid].get("scheduled", datetime.now(timezone.utc)))
        
        # Probe the URL for stages 4, 6, and 7
        probe(url, uid, pod_ready_ts)
        log.info(f"Completed pod {uid[:6]} – awaiting next revision...")


if __name__ == "__main__":
    main()
