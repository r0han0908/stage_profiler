#!/usr/bin/env python3
"""
Full life-cycle profiler for a Knative / Kubernetes pod.
Stages captured (spans):
  1. control-plane-scheduling
  2. image-pull
  3. container-creation
  4. network-warmup
  5. runtime-startup
  6. app-init
  7. first-request
"""

import argparse, os, sys, time, urllib.request
from datetime import datetime, timezone
from typing import Dict, Optional

from kubernetes import client, config as k8s_config
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# ───────────────────────── defaults ──────────────────────────
SERVICE   = os.getenv("SERVICE_NAME", "nginx")
NAMESPACE = os.getenv("NAMESPACE",   "default")
OTLP_EP   = os.getenv("OTEL_COLLECTOR_ENDPOINT",
                      "otel-collector.observability.svc.cluster.local:4317")

# ─────────────────── Kubernetes client ───────────────────────
try:
    k8s_config.load_incluster_config()
except Exception:
    k8s_config.load_kube_config()
v1 = client.CoreV1Api()
ev = client.CoreV1Api()       # events are in CoreV1 too

# ─────────────────── helper functions ────────────────────────
def resolve_pod(ns: str, svc: Optional[str], pod: Optional[str]) -> str:
    if pod:
        return pod
    pods = v1.list_namespaced_pod(
        ns, label_selector=f"serving.knative.dev/service={svc}"
    ).items
    if not pods:
        sys.exit(f"No pods found for service '{svc}' in namespace '{ns}'.")
    return pods[0].metadata.name


def http_ok(ip: str, port: int = 80, path: str = "/", tout: float = 0.3) -> bool:
    try:
        with urllib.request.urlopen(f"http://{ip}:{port}{path}", timeout=tout) as r:
            return 200 <= r.status < 400
    except Exception:
        return False

# ─────────────────────── main ────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--namespace", default=NAMESPACE)
    ap.add_argument("--service",   default=SERVICE)
    ap.add_argument("--pod")
    ap.add_argument("--endpoint",  default=OTLP_EP)
    args = ap.parse_args()

    pod_name = resolve_pod(args.namespace, args.service, args.pod)
    print(f"▶ full-stage profiler on pod {pod_name}")

    # tracer setup
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

    ts: Dict[str, datetime] = {}   # collected timestamps
    done = set()                   # spans already emitted

    # ─── polling loop ─────────────────────────────────────────
    while True:
        pod = v1.read_namespaced_pod(pod_name, args.namespace)

        # creation timestamp (always available)
        if "created" not in ts:
            ts["created"] = pod.metadata.creation_timestamp.replace(
                tzinfo=timezone.utc
            )

        # 1. control-plane scheduling (event “Scheduled”)
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

        # 2 & 3 use normal pod events
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

        # container status snapshots
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

        # ─── emit spans whenever both ends are known ──────────
        def emit(name: str, a: str, b: str) -> None:
            if (
                a in ts and b in ts
                and ts[a] is not None and ts[b] is not None
                and b not in done
                and ts[b] >= ts[a]
            ):
                start_ns = int(ts[a].timestamp() * 1e9)
                end_ns   = int(ts[b].timestamp() * 1e9)

                sp = tracer.start_span(name, start_time=start_ns)
                sp.end(end_time=end_ns)

                print(f"✓ {name:<25} {(ts[b] - ts[a]).total_seconds():7.3f}s")
                done.add(b)

        emit("control-plane-scheduling", "created",     "scheduled")
        emit("image-pull",               "pulling",     "pulled")
        emit("container-creation",       "pulled",      "created_cntr")
        emit("network-warmup",           "qp_run",      "qp_ready")
        emit("runtime-startup",          "qp_ready",    "user_run")
        emit("app-init",                 "user_run",    "user_ready")
        emit("first-request",            "user_ready",  "first_req")

        if len(done) == 7:
            print("🎉 all stages captured – exiting")
            tp.shutdown()
            break

        time.sleep(0.5)


if __name__ == "__main__":
    main()
