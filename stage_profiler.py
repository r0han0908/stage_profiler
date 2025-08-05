#!/usr/bin/env python3
"""
Knative cold‑start stage profiler (loops forever, auto‑detects service name)
--------------------------------------------------------------------------
Using Kubernetes API to detect all stages including stage 5 (runtime startup).
Includes robust pod detection for both existing and new pods.
"""

import os
import logging
import argparse
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

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
queue_proxy_ready_ts = {}  # When queue-proxy becomes ready

# Track which spans we've already emitted per pod
emitted_spans = {}  # uid -> set of emitted span names
# Track which pods we've already fully processed
processed = set()

# Kubernetes API client
v1_client = None  # Core API client

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
    
    # Ensure we have reasonable timestamps
    if end <= start:
        log.warning(f"Invalid timing for {name} [pod {uid[:6]}]: start={start}, end={end}, using 1ms")
        end = start.replace(microsecond=start.microsecond + 1000)
    
    # Start and end the span
    span = tracer.start_span(name, start_time=int(_utc(start).timestamp() * 1e9))
    span.end(end_time=int(_utc(end).timestamp() * 1e9))
    
    # Calculate and log the duration
    duration = (end - start).total_seconds()
    log.info("%s [%s]: %s", name, uid[:6], _ms(duration))

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
                        queue_proxy_ready_ts[uid] = end_time
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
    
    # Debug info - print pod details
    log.info(f"Processing pod: {pod_name} (uid: {uid[:6] if uid else 'None'}), phase: {pod.status.phase}")
    labels = pod.metadata.labels or {}
    label_str = ", ".join(f"{k}={v}" for k, v in labels.items())
    log.info(f"Pod labels: {label_str}")
    
    # Skip if already processed
    if uid in processed:
        log.debug(f"Pod {pod_name} already processed, skipping")
        return
    
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

        # Stage 2 - Image Pull - use ANY container (original logic)
        if "scheduled" in state and "running" not in state:
            for cs in pod.status.container_statuses or []:
                if cs.state and cs.state.running:
                    state["running"] = cs.state.running.started_at
                    
                    # Only emit stage2 once
                    if "stage2" not in emitted:
                        emitted.add("stage2")
                        emit_span("stage2-image-pull", state["scheduled"], state["running"], uid)
                    break

        # Stage 3 - Container Init & Ready
        for c in pod.status.conditions or []:
            if c.type == "Ready" and c.status == "True":
                state["ready"] = c.last_transition_time
                
                # Only emit stage3 once - using original logic
                if "stage3" not in emitted and "running" in state:
                    emitted.add("stage3")
                    emit_span("stage3-container-init", state["running"], c.last_transition_time, uid)
                
                # Start stage5 monitoring in a background thread
                if "stage5" not in emitted and "scheduled" in state:
                    threading.Thread(
                        target=monitor_queue_proxy_ready,
                        args=(namespace, pod_name, uid, scheduled_ts[uid]),
                        daemon=True
                    ).start()
                    emitted.add("stage5")  # Mark as started to prevent duplicates
                
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
    
    # First check for existing pods before starting watch
    try:
        log.info(f"Looking for existing pods in {ns} with selector '{selector or '<all>'}'")
        existing_pods = v1_client.list_namespaced_pod(namespace=ns, label_selector=selector)
        
        if existing_pods.items:
            log.info(f"Found {len(existing_pods.items)} existing pods:")
            for pod in existing_pods.items:
                log.info(f"  - {pod.metadata.name} (uid: {pod.metadata.uid[:6] if pod.metadata.uid else 'None'})")
                process_pod(pod)
        else:
            log.warning(f"No existing pods found with selector '{selector}' in namespace {ns}")
            
            # Show all pods in namespace to help with debugging
            all_pods = v1_client.list_namespaced_pod(namespace=ns)
            if all_pods.items:
                log.info(f"Found {len(all_pods.items)} total pods in namespace {ns}:")
                for pod in all_pods.items:
                    labels = pod.metadata.labels or {}
                    label_str = ", ".join(f"{k}={v}" for k, v in labels.items())
                    log.info(f"  - {pod.metadata.name}: {label_str}")
            else:
                log.info(f"No pods at all found in namespace {ns}")
    except Exception as e:
        log.error(f"Error listing existing pods: {e}")
    
    # Now start watching for new pod events
    resource_version = ""  # Start with no resource version to get all events
    
    while True:
        try:
            w = watch.Watch()
            log.info(f"Starting pod watch in {ns} with selector '{selector or '<all>'}'")
            
            for event in w.stream(
                v1_client.list_namespaced_pod,
                namespace=ns,
                label_selector=selector,
                resource_version=resource_version,
                timeout_seconds=60
            ):
                pod = event['object']
                event_type = event['type']
                
                # Update resource version for next iteration
                if hasattr(pod.metadata, 'resource_version') and pod.metadata.resource_version:
                    resource_version = pod.metadata.resource_version
                
                log.info(f"Pod event: {event_type} - {pod.metadata.name}")
                process_pod(pod)
                
        except client.rest.ApiException as e:
            if e.status == 410:  # Gone - resource version too old
                log.warning("Resource version too old, resetting watch")
                resource_version = ""
            else:
                log.error(f"API error in watch: {e}")
            time.sleep(1)
            
        except Exception as e:
            log.error(f"Watch error: {e}")
            time.sleep(5)
            
        # Do a full refresh periodically to ensure we didn't miss anything
        try:
            log.info("Performing periodic full pod refresh")
            pod_list = v1_client.list_namespaced_pod(namespace=ns, label_selector=selector)
            for pod in pod_list.items:
                process_pod(pod)
                
            if pod_list.metadata.resource_version:
                resource_version = pod_list.metadata.resource_version
        except Exception as e:
            log.error(f"Error during periodic refresh: {e}")
        
        log.info("Watch cycle completed, restarting watch")

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
            
            # Get the service name from detection
            svc_name = service_by_uid.get(uid)
            if svc_name:
                log.info(f"Processing pod {uid[:6]} for service '{svc_name}'")
            else:
                log.warning(f"No service name detected for pod {uid[:6]}")
            
            # Get the ready timestamp for this pod
            pod_ready_ts = pod_state[uid].get("ready", pod_state[uid].get("scheduled", datetime.now(timezone.utc)))

        # Probe the URL for stages 4, 6, and 7 (stage 5 is handled by the queue-proxy monitor)
        probe(args.url, uid, pod_ready_ts)
        log.info(f"Completed pod {uid[:6]} – awaiting next revision...")


if __name__ == "__main__":
    main()
