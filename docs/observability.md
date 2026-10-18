# Metrics and observability

CapMesh exposes Prometheus text metrics directly from each process. A
Prometheus installation and dashboard are optional: the endpoints can be
collected with `kubectl port-forward` and `curl` during a benchmark run.

## Endpoints

| Component | Metrics | pprof (disabled by default) |
| --- | --- | --- |
| server | `:19090/metrics` | `:16060/debug/pprof/` |
| agent | `:19091/metrics` | `:16061/debug/pprof/` |

The metrics listeners use a dedicated Prometheus registry and include Go
runtime and process metrics. The pprof listener is separate and only starts
when its address is configured.

## Collect a Kubernetes snapshot

Forward the server metrics endpoint:

```bash
kubectl -n capmesh port-forward deployment/capmesh-server 19090:19090
curl -fsS http://127.0.0.1:19090/metrics > server-before.prom
```

Agent metrics are per pod/node. Pick the agent on the node used by the
benchmark and collect it separately:

```bash
kubectl -n capmesh get pods -l app.kubernetes.io/name=capmesh-agent -o wide
kubectl -n capmesh port-forward pod/<agent-pod> 19091:19091
curl -fsS http://127.0.0.1:19091/metrics > agent-before.prom
```

Run the benchmark and repeat the two `curl` commands with `after` filenames.
Counters should be compared as `after - before`; gauges and histogram bucket
values are point-in-time snapshots. Keep the `.prom` files beside the
benchmark JSON result so every baseline remains reproducible.

For a quick, readable subset:

```bash
curl -fsS http://127.0.0.1:19090/metrics \
  | grep -E '^(capmesh_server|capmesh_recording|go_goroutines|process_resident_memory_bytes)'
```

## Important metrics

Agent pipeline:

- `capmesh_agent_packets_captured_total` and `capmesh_agent_capture_bytes_total`
- `capmesh_agent_batch_packets`, `capmesh_agent_batch_bytes`, and
  `capmesh_agent_batch_age_seconds`
- `capmesh_agent_batch_flush_total{reason=...}`
- `capmesh_agent_outbound_queue_depth`,
  `capmesh_agent_outbound_queue_overflows_total`, and
  `capmesh_agent_outbound_dropped_packets_total`
- `capmesh_agent_grpc_send_duration_seconds{type=...}` and
  `capmesh_agent_grpc_messages_total{type=...,result=...}`
- `capmesh_agent_capture_errors_total`, `capmesh_agent_stream_errors_total`,
  and `capmesh_agent_stream_reconnects_total`
- `capmesh_agent_capture_engine_selections_total{engine=...}` and
  `capmesh_agent_capture_engine_fallbacks_total{reason=...}`
- `capmesh_agent_afpacket_socket_packets_total`,
  `capmesh_agent_afpacket_socket_drops_total`, and
  `capmesh_agent_afpacket_queue_freezes_total`
- `capmesh_agent_afpacket_polls_total`,
  `capmesh_agent_afpacket_active_rings`, and
  `capmesh_agent_afpacket_ring_bytes`

Server pipeline:

- `capmesh_server_batches_received_total`,
  `capmesh_server_packets_received_total`, and `capmesh_server_batch_bytes`
- `capmesh_server_sequence_gap_events_total` and
  `capmesh_server_sequence_gap_packets_total`
- `capmesh_server_reorder_buffer_packets` and
  `capmesh_server_reorder_wait_seconds`
- `capmesh_server_fanout_duration_seconds`,
  `capmesh_server_fanout_deliveries_total`, and
  `capmesh_server_subscriber_queue_usage`
- `capmesh_server_late_packets_total` and
  `capmesh_server_subscriber_drops_total`
- `capmesh_server_rebatch_packets`, `capmesh_server_rebatch_bytes`,
  `capmesh_server_rebatch_age_seconds`, and
  `capmesh_server_rebatch_flush_total{reason=...}`

Recorder pipeline:

- `capmesh_recording_queue_depth`, `capmesh_recording_queue_usage`, and
  `capmesh_recording_queue_overflows_total`
- `capmesh_recording_write_duration_seconds`,
  `capmesh_recording_flush_duration_seconds`, and
  `capmesh_recording_sync_duration_seconds`
- `capmesh_recording_write_bytes_total`,
  `capmesh_recording_write_errors_total`, and
  `capmesh_recording_finalize_duration_seconds`

Go/process metrics such as `go_goroutines`, `go_memstats_alloc_bytes`,
`process_cpu_seconds_total`, and `process_resident_memory_bytes` are available
from both components.

The dumpcap backend does not expose kernel packet-drop statistics. AF_PACKET
reads `PACKET_STATISTICS`; its socket drop delta increments both
`capmesh_agent_afpacket_socket_drops_total` and the backend-neutral
`capmesh_agent_packets_dropped_total`. Continue to use sequence gaps and
benchmark loss as end-to-end signals because socket statistics do not include
drops later in the transport or subscriber pipeline.

## Sampling

Per-packet reorder-wait timing is sampled to limit hot-path overhead. The
server default is one sample per 1024 sequence numbers. Configure it with:

```text
CAPMESH_METRICS_SAMPLE_EVERY=1024
```

Use the same value for baseline and candidate runs. Set it to `1` only for a
short diagnostic run, not for a throughput baseline.

## CPU and heap profiles

Enable pprof only while diagnosing a controlled environment:

```bash
kubectl -n capmesh set env deployment/capmesh-server CAPMESH_SERVER_PPROF_LISTEN=:16060
kubectl -n capmesh rollout status deployment/capmesh-server
kubectl -n capmesh port-forward deployment/capmesh-server 16060:16060
go tool pprof -http=:0 'http://127.0.0.1:16060/debug/pprof/profile?seconds=30'
go tool pprof -http=:0 http://127.0.0.1:16060/debug/pprof/heap
```

For an agent, set `CAPMESH_AGENT_PPROF_LISTEN=:16061` on the DaemonSet and
port-forward the selected pod. Disable profiling after collection by restoring
the variables to an empty value. The pprof ports are not exposed by a Service.
