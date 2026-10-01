# Continuous server-owned capture

Continuous capture is a singleton recording job owned by `capmesh-server`. The
client starts the job with one unary RPC and can exit immediately; closing the
client does not stop packet capture or file recording.

## Prerequisites

Server-side recording must be enabled, and both limits must be positive:

```yaml
CAPMESH_RECORD_DIR: /app/captures
CAPMESH_RECORD_SEGMENT_SIZE: 100MiB
CAPMESH_RECORD_MAX_SESSION_SIZE: 10GiB
```

The segment size must not exceed the total size. CapMesh currently keeps this
singleton state in memory, so the server deployment must remain at one replica.

## Start, inspect, and stop

The start command accepts the same interface and workload selectors as a normal
session. It prints the session ID and exits without subscribing to the packet
stream.

```bash
capmesh-client \
  --server capmesh-server:18443 \
  --continuous-start \
  --nodes worker-01,worker-02 \
  --interface uplink \
  --namespace payment \
  --workload-kind statefulset \
  --workload-name payment-worker \
  --direction both \
  --follow=true \
  --filter "tcp port 443"
```

Only one continuous capture can be active. Another start request returns gRPC
`AlreadyExists`; callers must stop the current job before starting a new one.

```bash
capmesh-client --server capmesh-server:18443 --continuous-status
capmesh-client --server capmesh-server:18443 --continuous-stop
```

An admin or shared token is required to start and stop. Viewer, admin, and
shared tokens can read status. To inspect the live packets in Wireshark without
affecting recording, use the returned session ID:

```bash
capmesh-client --server capmesh-server:18443 --session SESSION_ID | wireshark -k -i -
```

Disconnecting this streaming client leaves the server-owned job running.

## File layout and retention

Files are stored in one rolling directory:

```text
<record-dir>/continuous/
  metadata.json
  trace-<start-time>-<session-id>-000001.pcapng
  trace-<start-time>-<session-id>-000002.pcapng
  trace-<start-time>-<session-id>-000003.pcapng.part
```

The `.part` file is the segment currently being written. A completed segment is
renamed to `.pcapng`. Before opening another segment, CapMesh removes the oldest
completed files until there is room under `record-max-session-size`; capture
then continues. `metadata.json` contains the retained segment list and current
job status. PCAPNG framing and the last packet batch can make the on-disk size
briefly exceed the configured boundary by a small amount while the active
`.part` file is finalized.

Normal, TTL-based sessions keep their existing behavior: reaching the total
limit ends that session as `TRUNCATED`; they do not delete old files.

After a server restart, finalized continuous files remain on the volume but the
job is not resumed automatically. A new start removes stale `.part` files,
includes previous finalized segments in retention, and prunes the oldest files
when necessary.

The retention counters are exported as:

- `capmesh_continuous_recording_deleted_segments_total`
- `capmesh_continuous_recording_deleted_bytes_total`
