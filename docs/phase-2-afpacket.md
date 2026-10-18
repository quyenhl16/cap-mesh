# Phase 2 AF_PACKET capture engine

The Linux agent can capture through an `AF_PACKET` socket backed by a
`TPACKET_V3` mmap ring. The server, protobuf protocol, recorder, and benchmark
client continue to consume the Phase 1 `CapturedBatch` contract.

## Data path and ownership

```text
NIC -> Linux network stack -> TPACKET_V3 ring -> batch arena -> gRPC pipeline
```

The reader uses the ring data only until the next ring read. Every accepted
packet is copied once into an agent-owned batch arena before the kernel block
can be reused. Ring-backed slices never leave the capture adapter.

## Engine modes

`CAPMESH_CAPTURE_ENGINE` accepts:

- `afpacket`: strict mode; fail the capture if AF_PACKET cannot start.
- `dumpcap`: use the Phase 0/1 subprocess backend.
- `auto`: prefer AF_PACKET and use dumpcap only when initialization fails.

Fallback is allowed only before the first packet. A runtime AF_PACKET error
fails the source instead of switching data paths and risking gaps or duplicate
packets. Use strict modes for benchmarks so the measured backend is explicit.

## Ring configuration

```text
CAPMESH_CAPTURE_RING_SIZE=64MiB
CAPMESH_CAPTURE_BLOCK_SIZE=1MiB
CAPMESH_CAPTURE_RING_BUDGET=512MiB
CAPMESH_CAPTURE_BLOCK_TIMEOUT=1ms
CAPMESH_CAPTURE_POLL_TIMEOUT=1ms
```

Ring size is per active capture source. The shared budget prevents concurrent
sessions from allocating more mmap ring memory than the agent is configured to
allow. Ring size must be divisible by block size; block size must be divisible
by the host page size and the internal 4096-byte frame size. Block and poll
timeouts must not exceed the agent batch delay.

## Capture filters

The socket consumes classic BPF instructions. For a non-empty libpcap capture
filter, the agent invokes `dumpcap -ddd` once while starting the capture, parses
the decimal cBPF program, caches it, and attaches it with `SO_ATTACH_FILTER`.
Packet capture itself does not pass through dumpcap. An empty filter becomes a
single kernel return instruction so snaplen is still applied before data is
copied into the ring.

## Kubernetes benchmark

The existing DaemonSet already grants `NET_RAW` and `NET_ADMIN`; privileged
mode, host PID, and an additional capture pod are not required.

Run the dumpcap baseline:

```bash
kubectl apply -k deploy/overlays/benchmark-dumpcap
kubectl -n capmesh rollout status daemonset/capmesh-agent
```

Save the benchmark result and metrics, then run the AF_PACKET candidate:

```bash
kubectl apply -k deploy/overlays/benchmark-afpacket
kubectl -n capmesh rollout status daemonset/capmesh-agent
```

Use the same images, scenario, node placement, resource limits, warm-up, and
trial count for both runs. Test 64, 512, 1500, and mixed packet sizes, followed
by one-interface and multi-interface runs. Ring sizes of 64MiB, 128MiB, and
256MiB can then be compared without changing the binary.
