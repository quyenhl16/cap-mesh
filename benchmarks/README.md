# CapMesh Kubernetes benchmark

The benchmark uses one binary in two roles:

```text
runner Job -> UDP -> receiver Deployment
     \-> CapMesh session -> agent -> server -> runner subscriber
```

The runner asks CapMesh to capture ingress traffic for the receiver Deployment.
It embeds a run ID, sequence number, and send timestamp in every packet, then
reports throughput, loss, duplicate packets, ordering, and end-to-end latency.

## Build the image

From the repository root:

```bash
docker build -f Dockerfile.benchmark \
  --build-arg VERSION=0.1.0 \
  --build-arg GIT_COMMIT=$(git rev-parse --short HEAD) \
  -t capmesh-benchmark:latest .
```

Push the image to the registry used by the cluster, or load it into the local
cluster. Update the image in `k8s/receiver-deployment.yaml` and
`k8s/runner-job.yaml` when it is not available as `capmesh-benchmark:latest`.

The manifests use the existing `capmesh` namespace and expect these CapMesh
secrets to exist:

- `capmesh-auth`, key `token`.
- `capmesh-tls`, key `ca.crt`.

For an insecure development deployment, remove the secret mounts and replace
the TLS/token arguments in the runner Job with `--insecure`.

## Deploy the receiver

```bash
kubectl apply -k benchmarks/k8s
kubectl -n capmesh rollout status deployment/capmesh-benchmark-receiver
```

Keep the receiver at one replica. The runner targets this Deployment with
`maxPods=1` so each generated packet has one controlled destination.

## Run a benchmark

Select a strict capture backend before collecting a comparison. This prevents
the `auto` mode from silently measuring dumpcap after an AF_PACKET startup
failure:

```bash
# Baseline
kubectl apply -k deploy/overlays/benchmark-dumpcap

# Candidate, after saving the baseline result and metric snapshots
kubectl apply -k deploy/overlays/benchmark-afpacket
```

Wait for `daemonset/capmesh-agent` to finish rolling out after each change.

The default Job runs `scenarios/baseline.yaml`:

```bash
kubectl -n capmesh delete job capmesh-benchmark-runner --ignore-not-found
kubectl apply -f benchmarks/k8s/runner-job.yaml
kubectl -n capmesh logs -f job/capmesh-benchmark-runner
```

To run another built-in scenario, change the `--scenario` argument in
`runner-job.yaml` to one of:

- `/benchmarks/scenarios/small-packets.yaml`
- `/benchmarks/scenarios/large-packets.yaml`
- `/benchmarks/scenarios/fanout.yaml`

Every scenario setting can be overridden by a command-line flag. For example:

```yaml
args:
  - run
  - --scenario=/benchmarks/scenarios/baseline.yaml
  - --packet-rate=50000
  - --duration=5m
  - --version-label=phase-1-rebatch
```

The human-readable report is written to the Job log. The JSON result is also
written to the Kubernetes termination message, which avoids requiring a PVC or
tools inside the distroless image. Save it locally with:

```bash
kubectl -n capmesh get pod \
  -l job-name=capmesh-benchmark-runner \
  -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.message}' \
  > result.json
```

The Job is retained for one hour after completion so the result can be copied.

For performance diagnostics, save server and agent Prometheus snapshots before
and after each run. The port-forward commands and the metrics to compare are in
[`docs/observability.md`](../docs/observability.md).

## Compare two runs

Build the binary locally with `make benchmark`, then run:

```bash
bin/capmesh-benchmark compare \
  --base results/0.1.0-baseline.json \
  --candidate results/phase-1-rebatch.json
```

Use the same scenario, cluster, node placement, CapMesh configuration, and
resource limits for both runs. Run a scenario several times before treating a
small difference as an improvement or regression.

## Scenario fields

```yaml
name: baseline-512b-10kpps
receiver: capmesh-benchmark-receiver:9000
targetNamespace: capmesh
targetKind: deployment
targetName: capmesh-benchmark-receiver
packetSize: 512
packetRate: 10000
duration: 1m
warmup: 10s
settle: 2s
drain: 2s
subscribers: 1
snaplen: 65535
reorderWindow: 300ms
maxLossPercent: 0.1
maxP99Latency: 100ms
```

`packetSize` is the intended Ethernet frame size without FCS. The current
generator uses IPv4/UDP, so the UDP payload is `packetSize - 42` bytes. The
minimum supported size is 62 bytes.

The runner marks the generator as saturated when its actual send rate is below
95% of the requested rate. Saturated runs should not be used as a CapMesh
capacity baseline.
