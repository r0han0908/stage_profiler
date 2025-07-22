#!/usr/bin/env python3
"""
Full life‑cycle profiler for a Knative / Kubernetes pod.

Stages captured (spans)
  1. control‑plane‑scheduling
  2. image‑pull
  3. container‑creation
  4. network‑warmup
  5. runtime‑startup
  6. app‑init
  7. first‑request

NEW  ›  a configurable warm‑up delay (default 10 s) before monitoring starts.
"""

from __future__ import annotations  # safe even on py3.8

import argparse, os, sys, time, urllib.request
from datetime import datetime, timezone
from typing import Dict, Optional

from kubernetes import client, config as k8s_config
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# ─── defaults ──────────────────────────────────────────────────────────────
SERVICE   = os.getenv("SERVICE_NAME", "nginx")
NAMESPACE = os.getenv("NAMESPACE",   "default")
OTLP_EP   = os.getenv("OTEL_COLLECTOR_ENDPOINT",
                      "otel-collector.observability.svc.cluster.local:4317")
WARMUP_DEFAULT = int(os.getenv("WARMUP_SECS", "10"))

# ─── Kubernetes client ─────────────────────────────────────────────────────
try:
    k8s_config.load_incluster_config()
except Exception:
    k8s_config.load_kube_config()
v1 = client.CoreV1Api()
ev = client.CoreV1Api()          # Events live in CoreV1

# ─── helpers ───────────────────────────────────────────────────────────────
def resolve_pod(ns: str, service: str | None, pod_prefix: str | None) -> str:
    """Return the first pod name that matches either label or prefix."""
    if pod_prefix:
        pods = v1.list_namespaced_pod(ns).items
        for p in pods:
            if pod_prefix in p.metadata.name:
                return p.metadata.name
        sys.exit(f"No pod name containing '{pod_prefix}' found in ns '{ns}'.")
    # label‑based lookup (Knative service)
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

# ─── main ──────────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--namespace", default=NAMESPACE)
    ap.add_argument("--service",   default=SERVICE,
                    help="Knative service name (for label lookup)")
    ap.add_argument("--pod",
                    help="Substring to match pod names (skips label lookup)")
    ap.add_argument("--endpoint",  default=OTLP_EP,
                    help="OTLP gRPC collector endpoint")
    ap.add_argument("--warmup", type=int, default=WARMUP_DEFAULT,
                    help=f"seconds to wait before monitoring (default {WARMUP_DEFAULT})")
    args = ap.parse_args()

    # Warm‑up delay ---------------------------------------------------------
    if args.warmup > 0:
        print(f"Waiting {args.warmup}s before starting profiling …")
        time.sleep(args.warmup)

    pod_name = resolve_pod(args.namespace, args.service, args.pod)
    print(f"Profiling pod {pod_name}")

    # Tracer setup ----------------------------------------------------------
    tp = TracerProvider(
        resource=Resource.create({
            "service.name":      args.service,
            "k8s.namespace":     args.namespace,
            "k8s.pod.name":      pod_name,
        })
    )
    tp.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=args.endpoint, insecure=True)
        )
    )
    trace.set_tracer_provider(tp)
    tracer = trace.get_tracer(__name__)

    ts: Dict[str, datetime] = {}   # timestamps collected
    done = set()                  # spans already emitted

    # Poll loop -------------------------------------------------------------
    while True:
        pod = v1.read_namespaced_pod(pod_name, args.namespace)

        # creation timestamp
        ts.setdefault("created",
            pod.metadata.creation_timestamp.replace(tzinfo=timezone.utc))

        # 1. control‑plane scheduling
        if "scheduled" not in ts:
            evs = ev.list_namespaced_event(
                args.namespace,
                field_selector=f"involvedObject.name={pod_name},involvedObject.kind=Pod"
            ).items
            for e in evs:
                if e.reason == "Scheduled":
                    ts["scheduled"] = (e.last_timestamp or e.event_time).replace(
                        tzinfo=timezone.utc
                    )
                    break

        # 2 & 3 image pull / container create events
        if {"pulling", "pulled", "created_cntr"} - ts.keys():
            for e in evs:
                if e.reason == "Pulling" and "pulling" not in ts:
                    ts["pulling"] = (e.last_timestamp or e.event_time).replace(
                        tzinfo=timezone.utc
                    )
                if e.reason == "Pulled" and "pulled" not in ts:
                    ts["pulled"] = (e.last_timestamp or e.event_time).replace(
                        tzinfo=timezone.utc
                    )
                if e.reason == "Created" and "created_cntr" not in ts:
                    ts["created_cntr"] = (e.last_timestamp or e.event_time).replace(
                        tzinfo=timezone.utc
                    )

        # container statuses
        qp = next((cs for cs in pod.status.container_statuses
                   if cs.name == "queue-proxy"), None)
        uc = next((cs for cs in pod.status.container_statuses
                   if cs.name == "user-container"), None)

        if qp and qp.state.running and "qp_run" not in ts:
            ts["qp_run"] = qp.state.running.started_at.replace(tzinfo=timezone.utc)
        if qp and qp.ready and "qp_ready" not in ts:
            ts["qp_ready"] = datetime.now(timezone.utc)

        if uc and uc.state.running and "user_run" not in ts:
            ts["user_run"] = uc.state.running.started_at.replace(tzinfo=timezone.utc)
        if uc and uc.ready and "user_ready" not in ts:
            ts["user_ready"] = datetime.now(timezone.utc)

        # first request
        if "user_ready" in ts and "first_req" not in ts:
            if http_ok(pod.status.pod_ip):
                ts["first_req"] = datetime.now(timezone.utc)

        # Emit spans when both ends known -------------------------------
        def emit(name: str, a: str, b: str) -> None:
            if {a, b} <= ts.keys() and b not in done and ts[b] >= ts[a]:
                start_ns = int(ts[a].timestamp() * 1e9)
                end_ns   = int(ts[b].timestamp() * 1e9)
                span = tracer.start_span(name, start_time=start_ns)
                span.end(end_time=end_ns)
                print(f"{name:<23} {(ts[b]-ts[a]).total_seconds():8.3f}s")
                done.add(b)

        emit("control-plane-scheduling", "created",     "scheduled")
        emit("image-pull",               "pulling",     "pulled")
        emit("container-creation",       "pulled",      "created_cntr")
        emit("network-warmup",           "qp_run",      "qp_ready")
        emit("runtime-startup",          "qp_ready",    "user_run")
        emit("app-init",                 "user_run",    "user_ready")
        emit("first-request",            "user_ready",  "first_req")

        if len(done) == 7:
            print("All stages captured – exiting")
            tp.shutdown()
            break

        time.sleep(0.5)


if __name__ == "__main__":
    main()
