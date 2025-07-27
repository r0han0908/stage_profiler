#!/usr/bin/env python3
"""
Fetch the most recent trace for SERVICE from Jaeger HTTP API and
extract durations for these spans (stages 4–7):
  4. network-warmup
  5. runtime-startup
  6. app-init
  7. first-request

Tries, in order:
  1. jaeger-ui.observability.svc.cluster.local:16686
  2. localhost:31686
  3. --fallback-query (if provided)
"""

import argparse
import os
import sys
import socket
import requests

DEFAULT_CLUSTER = "jaeger-ui.observability.svc.cluster.local:16686"
DEFAULT_LOCAL   = "localhost:31686"
LIMIT           = 1  # most recent trace

def try_connect(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        ip = socket.gethostbyname(host)
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception:
        return False

def choose_endpoint(candidates):
    for ep in candidates:
        if ":" not in ep:
            continue
        host, port = ep.split(":", 1)
        print(f"Trying Jaeger endpoint {ep} …")
        if try_connect(host, int(port)):
            print(f"Using Jaeger endpoint: {ep}")
            return ep
        else:
            print(f"Failed to connect to {ep}")
    sys.exit("No reachable Jaeger endpoint found")

def fetch_latest_trace(base_url: str, service: str):
    url = f"http://{base_url}/api/traces"
    params = {"service": service, "limit": LIMIT}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
    except requests.RequestException as e:
        sys.exit(f"Error querying Jaeger ({url}): {e}")
    data = r.json().get("data", [])
    if not data:
        sys.exit(f"No traces found in Jaeger for service '{service}'")
    return data[0]

def extract_stage(spans, name):
    for s in spans:
        if s.get("operationName") == name:
            return s.get("duration", 0) / 1e6
    return None

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--fallback-query",
                   help="Fallback Jaeger endpoint host:port")
    p.add_argument("--service", default=os.getenv("SERVICE_NAME", "nginx"))
    args = p.parse_args()

    candidates = [DEFAULT_CLUSTER, DEFAULT_LOCAL]
    if args.fallback_query:
        candidates.append(args.fallback_query)
    endpoint = choose_endpoint(candidates)

    trace = fetch_latest_trace(endpoint, args.service)
    trace_id = trace.get("traceID", "<unknown>")
    spans = trace.get("spans", [])

    print(f"Trace ID: {trace_id}, spans: {len(spans)}\n")

    for stage in ("network-warmup", "runtime-startup", "app-init", "first-request"):
        dur = extract_stage(spans, stage)
        if dur is None:
            print(f"{stage:<15} NOT FOUND")
        else:
            print(f"{stage:<15} {dur:8.3f}s")

if __name__ == "__main__":
    main()
