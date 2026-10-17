# Phase 1 batch pipeline

Phase 1 prepares the data path for the AF_PACKET/TPACKET_V3 capture engine. It
keeps dumpcap as the active capture backend while changing the ownership and
queue contracts from individual packets to immutable batches.

## Data path

```text
dumpcap parser
  -> CapturedBatch (packet/byte/time bounded)
  -> bounded agent outbound queue
  -> gRPC
  -> per-packet reorder heap
  -> immutable rebatch (packet/byte/time bounded)
  -> independent bounded subscriber and recorder queues
```

The capture engine owns its source memory. Before publishing a
`CapturedBatch`, it must place packet payloads in agent-owned memory. A
published batch is immutable: consumers must not modify or append to its
packet or payload slices. This lets a future TPACKET_V3 engine return a ring
block to the kernel immediately after copying the selected packets once.

Server fan-out sends the same immutable packet payloads to every subscriber
and the recorder. Queue entries contain slice references; packet payloads are
not deep-copied per consumer.

## Configuration

Agent capture batching:

```text
CAPMESH_AGENT_BATCH_MAX_PACKETS=256
CAPMESH_AGENT_BATCH_MAX_BYTES=1048576
CAPMESH_AGENT_BATCH_MAX_DELAY=1ms
```

Server post-reorder batching:

```text
CAPMESH_SERVER_REBATCH_MAX_PACKETS=256
CAPMESH_SERVER_REBATCH_MAX_BYTES=1048576
CAPMESH_SERVER_REBATCH_MAX_DELAY=1ms
```

The packet and byte limits bound memory and transport message size. The delay
limit lets low-rate traffic flush without waiting for a full batch. Baseline
and candidate benchmark runs must use the same values.

Subscriber and recorder queue sizes now count immutable batches rather than
individual packets. The base deployment uses 256 entries for each; increasing
these values can retain up to `queue entries * maximum batch bytes` per slow
consumer.

## Backpressure

- The capture and gRPC outbound queues are bounded.
- A capture batch arriving while the gRPC outbound queue is full is dropped;
  capture continues and the drop is exported as a metric.
- Live subscriber queues never block session fan-out.
- Loss-aware subscribers, including the recorder, are disconnected on queue
  overflow so the recording is finalized as partial.
- A slow recorder does not block live subscribers or agent ingestion.

Important overflow signals are:

```text
capmesh_agent_outbound_queue_overflows_total
capmesh_agent_outbound_dropped_packets_total
capmesh_agent_outbound_disconnect_dropped_batches_total
capmesh_server_subscriber_drops_total
capmesh_recording_queue_overflows_total
```

## Phase 2 integration contract

The AF_PACKET engine will consume TPACKET_V3 blocks and emit the same
`CapturedBatch` type. It may use block boundaries as a batching hint, but it
must still enforce the configured packet, byte, and delay limits. Ring-backed
payloads must not escape after ownership of a block is returned to the kernel.
