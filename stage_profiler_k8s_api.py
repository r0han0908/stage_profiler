#!/usr/bin/env python3
"""
Extract the first three cold‑start stages via Kubernetes API:
  1. control‑plane‑scheduling
  2. image‑pull
  3. container‑creation
"""

import os
import sys
import time
from datetime import datetime, timezone
from kubernetes import client, config as k8s_config

# ─── Configuration ────────────────────────────────────────────
SERVICE   = os.getenv("SERVICE_NAME", "nginx")
NAMESPACE = os.getenv("NAMESPACE",    "default")
WARMUP    = int(os.getenv("WARMUP_SECS",     "10"))
TIMEOUT   = int(os.getenv("PROFILER_TIMEOUT", "180"))

# ─── Kubernetes client init ───────────────────────────────────
try:
    k8s_config.load_incluster_config()
except Exception:
    k8s_config.load_kube_config()
v1 = client.CoreV1Api()
ev = client.CoreV1Api()

# ─── Helpers ──────────────────────────────────────────────────
def resolve_pod():
    pods = v1.list_namespaced_pod(
        NAMESPACE,
        label_selector=f"serving.knative.dev/service={SERVICE}"
    ).items
    if not pods:
        sys.exit(f"No pods found for service '{SERVICE}' in namespace '{NAMESPACE}'")
    return pods[0].metadata.name

# ─── Main ────────────────────────────────────────────────────
if __name__ == "__main__":
    if WARMUP > 0:
        print(f"Waiting {WARMUP}s before starting profiling …")
        time.sleep(WARMUP)

    pod_name = resolve_pod()
    print(f"Profiling pod (API-only): {pod_name}")

    ts = {}
    done = set()
    start_time = datetime.now(timezone.utc)

    while True:
        now = datetime.now(timezone.utc)
        if (now - start_time).total_seconds() > TIMEOUT:
            print("Timeout reached; some early stages missing.")
            break

        pod = v1.read_namespaced_pod(pod_name, NAMESPACE)
        ts.setdefault("created", pod.metadata.creation_timestamp.replace(tzinfo=timezone.utc))

        events = ev.list_namespaced_event(
            NAMESPACE,
            field_selector=f"involvedObject.name={pod_name},involvedObject.kind=Pod"
        ).items

        for e in events:
            t = (e.last_timestamp or e.event_time)
            if not t:
                continue
            t = t.replace(tzinfo=timezone.utc)
            if e.reason == "Scheduled":
                ts.setdefault("scheduled", t)
            elif e.reason == "Pulling":
                ts.setdefault("pulling", t)
            elif e.reason == "Pulled":
                ts.setdefault("pulled", t)
            elif e.reason == "Created":
                ts.setdefault("created_cntr", t)

        def emit(label, a, b):
            if a in ts and b in ts and b not in done:
                print(f"{label:<22} {(ts[b] - ts[a]).total_seconds():.3f}s")
                done.add(b)

        emit("control-plane-scheduling", "created",      "scheduled")
        emit("image-pull",               "pulling",      "pulled")
        emit("container-creation",       "pulled",       "created_cntr")

        if len(done) == 3:
            print("First three stages captured.")
            break

        time.sleep(0.5)
