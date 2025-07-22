#!/usr/bin/env python3
"""
Full life‑cycle profiler for a Knative / Kubernetes pod.

Stages captured
────────────────
1. control‑plane‑scheduling
2. image‑pull
3. container‑creation
4. network‑warmup
5. runtime‑startup
6. app‑init
7. first‑request
"""

import argparse, json, os, sys, time, urllib.request
from datetime import datetime, timezone
from typing import Dict, List, Optional  # ← 3.8‑style typing

from kubernetes import client, config as k8s_config
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# ────────────── defaults ───────────────────────────────────
SERVICE   = os.getenv("SERVICE_NAME", "nginx")
NAMESPACE = os.getenv("NAMESPACE",   "default")
OTLP_EP   = os.getenv(
    "OTEL_COLLECTOR_ENDPOINT",
    "otel-collector.observability.svc.cluster.local:4317",
)
LOG_PATH  = os.getenv("STAGE_PROFILER_LOG", "stage_profiler.log")

# ────────────── Kubernetes client ─────────────────────────
try:
    k8s_config.load_incluster_config()
except Exception:
    k8s_config.load_kube_config()
v1 = client.CoreV1Api()
ev = client.CoreV1Api()   # events API

# ────────────── helpers ───────────────────────────────────
def first_pod_matching(namespace: str, substr: str) -> Optional[str]:
    for p in v1.list_namespaced_pod(namespace=namespace).items:
        if substr in p.metadata.name:
            return p.metadata.name
    return None


def resolve_pod(ns: str, service: Optional[str], pod_prefix: Optional[str]) -> str:
    if pod_prefix:
        match = first_pod_matching(ns, pod_prefix)
        if match:
            return match
        sys.exit(f"No pod whose name contains '{pod_prefix}' in ns '{ns}'.")
    pods = v1.list_namespaced_pod(
        ns, label_selector=f"serving.knative.dev/service={service}"
    ).items
    if not pods:
        sys.exit(f"No pods found for service '{service}' in ns '{ns}'.")
    return pods[0].metadata.name


def http_ok(ip: str, port: int = 80, path: str = "/", tout: float = 0.3) -> bool:
    try:
        with urllib.request.urlopen(f"http://{ip}:{port}{path}", timeout=tout) as r:
            return 200 <= r.status < 400
    except Exception:
        return False


def log_json(entry: Dict) -> None:
    with open(LOG_PATH, "a", buffering=1) as fp:
        fp.write(json.dumps(entry) + "\n")


# ────────────── profiler ──────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--namespace", default=NAMESPACE)
    ap.add_argument("--service",   default=SERVICE)
    ap.add_argument("--pod", help="substring / prefix to match pod name")
    ap.add_argument("--endpoint",  default=OTLP_EP)
    args = ap.parse_args()

    pod_name = resolve_pod(args.namespace, args.service, args.pod)
    print(f"▶ profiling pod {pod_name}")

    # OpenTelemetry tracer setup
    tp = TracerProvider(
        resource=Resource.create({
            "service.name":       args.service,
            "k8s.namespace.name": args.namespace,
            "k8s.pod.name":       pod_name,
        })
    )
    tp.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=args.endpoint, insecure=True)
        )
    )
    trace.set_tracer_provider(tp)
    tracer = trace.get_tracer(__name__)

    ts: Dict[str, datetime] = {}
    done: List[str] = []

    while True:
        pod = v1.read_namespaced_pod(pod_name, args.namespace)

        ts.setdefault("created",
                      pod.metadata.creation_timestamp.replace(tzinfo=timezone.utc))

        # events
        events = ev.list_namespaced_event(
            args.namespace,
            field_selector=f"involvedObject.name={pod_name},involvedObject.kind=Pod"
        ).items
        for e in events:
            when = (e.last_timestamp or e.event_time)
            if when:
                when = when.replace(tzinfo=timezone.utc)
            if e.reason == "Scheduled":
                ts.setdefault("scheduled", when)
            elif e.reason == "Pulling":
                ts.setdefault("pulling", when)
            elif e.reason == "Pulled":
                ts.setdefault("pulled", when)
            elif e.reason == "Created":
                ts.setdefault("created_cntr", when)

        # container statuses
        qp = next((cs for cs in pod.status.container_statuses or []
                   if cs.name == "queue-proxy"), None)
        uc = next((cs for cs in pod.status.container_statuses or []
                   if cs.name == "user-container"), None)

        if qp and qp.state.running:
            ts.setdefault("qp_run", qp.state.running.started_at.replace(tzinfo=timezone.utc))
        if qp and qp.ready:
            ts.setdefault("qp_ready", datetime.now(timezone.utc))
        if uc and uc.state.running:
            ts.setdefault("user_run", uc.state.running.started_at.replace(tzinfo=timezone.utc))
        if uc and uc.ready:
            ts.setdefault("user_ready", datetime.now(timezone.utc))

        if "user_ready" in ts and "first_req" not in ts and http_ok(pod.status.pod_ip):
            ts["first_req"] = datetime.now(timezone.utc)

        # emit spans
        def emit(stage: str, a: str, b: str) -> None:
            if stage in done or a not in ts or b not in ts:
                return
            dur = (ts[b] - ts[a]).total_seconds()
            with tracer.start_as_current_span(stage,
                                              start_time=int(ts[a].timestamp() * 1e9)) as sp:
                sp.set_attribute("duration_s", round(dur, 3))
                sp.end(end_time=int(ts[b].timestamp() * 1e9))
            print(f"✓ {stage:<23} {dur:7.3f}s")
            log_json({
                "stage": stage,
                "pod":   pod_name,
                "start": ts[a].isoformat(),
                "end":   ts[b].isoformat(),
                "duration_s": round(dur, 3)
            })
            done.append(stage)

        emit("control-plane-scheduling", "created",     "scheduled")
        emit("image-pull",               "pulling",     "pulled")
        emit("container-creation",       "pulled",      "created_cntr")
        emit("network-warmup",           "qp_run",      "qp_ready")
        emit("runtime-startup",          "qp_ready",    "user_run")
        emit("app-init",                 "user_run",    "user_ready")
        emit("first-request",            "user_ready",  "first_req")

        if len(done) == 7:
            print("🎉 all 7 stages captured – exiting")
            tp.shutdown()
            break

        time.sleep(0.5)


if __name__ == "__main__":
    main()
