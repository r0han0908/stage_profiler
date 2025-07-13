# stage_profiler
`stage_profiler` is a lightweight instrumentation library for serverless functions.  
Drop it into your CI/CD pipeline or APM stack to automatically capture per-stage metrics—scheduling, image pull, container init, networking, runtime startup, app init, and first-request latency—so you can pinpoint and optimize cold-start bottlenecks. 

# Serverless Function Deployment Stages

A quick overview of the seven steps in a cold-start deployment:

1. **Control-Plane Scheduling**  
   Submit the deployment request; control plane allocates resources (~10–50 ms).

2. **Image Pull & Unpack**  
   Fetch and extract container layers (~100 ms–2 s).

3. **Container Creation & Init**  
   Instantiate the container runtime and apply hooks (~50–200 ms).

4. **Networking / Proxy Warm-Up**  
   Configure CNI, start or warm sidecar proxies (~50–100 ms).

5. **Runtime Start-Up**  
   Load language runtime (e.g., Node, Python, JVM) (~20–500 ms).

6. **App-Level Init**  
   Bootstrap your code: load configs, open DB connections (~100 ms–1 s).

7. **First-Request Overhead**  
   Execute handler, JIT compile or lazy-load modules (~50 ms–1 s).

_Last updated: July 13, 2025_  
