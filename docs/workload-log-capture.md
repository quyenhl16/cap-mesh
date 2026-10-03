# Continuous workload log capture

Workload log capture is a server-owned background job. A client starts it once
and may exit immediately. The job follows Pods belonging to one or more
StatefulSets or Deployments until it is explicitly stopped or the server exits.

Packet continuous capture and workload log capture are independent singletons,
so both can run at the same time. Only one workload log capture job can be
active globally; one job can contain multiple workload targets.

## Start, inspect, and stop

```bash
export CAPMESH_TOKEN='replace-me'

./bin/capmesh-client start logcapture \
  --workload pramf01/statefulset/mm \
  --workload pramf01/deployment/api \
  --container app \
  --log-since 5m \
  --max-pods 100

./bin/capmesh-client get logcapture

./bin/capmesh-client stop logcapture
```

Repeat `--workload` to capture several workloads in the same singleton job.
Repeat `--container` to select containers; omit it to capture all regular
application containers. Init containers are not captured. `--log-since 0`
captures only logs produced from the job start time.

A second `start logcapture` returns gRPC `AlreadyExists`. Stop the current job before
starting another. Admin/shared tokens may start and stop; viewer tokens may only
read status.

## Raw log format

The server calls the Kubernetes Pod Log API with `follow=true` and
`timestamps=false`. It writes response bytes unchanged: no JSON envelope, no
CapMesh timestamp, and no Pod/container prefix. Each resulting file therefore
has the same content format as:

```bash
kubectl logs -n NAMESPACE POD -c CONTAINER > file.log
```

Each Pod/container has a separate stream, so lines from different sources are
never merged into one file.

## File layout

```text
<record-dir>/logs/continuous/<run-id>/
  metadata.json
  <namespace>/
    <kind>-<workload>/
      <pod-name>_<pod-uid-first-8>/
        <container>/
          restart-<restart-count>/
            log-000001.log
            log-000002.log.part
```

An in-place container restart keeps the Pod UID but writes to a new
`restart-N` directory. If Kubernetes recreates a Pod with the same name, the new
UID creates a new Pod directory. Empty directories left by retention cleanup
are removed.

The `.part` file is active. Rotation occurs at a log-line boundary when it
reaches `CAPMESH_RECORD_SEGMENT_SIZE` (10 MiB by default). A single line larger
than the limit is kept intact. The total retained log data across all runs is
bounded by `CAPMESH_RECORD_MAX_SESSION_SIZE` (10 GiB by default); the oldest
finalized `.log` segments are deleted first. Packet PCAP quota and workload-log
quota are independent.

`metadata.json` is CapMesh job metadata and is not part of the captured log.

## Kubernetes permissions and behavior

The server ServiceAccount needs `get` on `pods/log`, plus its existing read
permissions for Pods, StatefulSets, Deployments, and ReplicaSets. The base RBAC
manifest includes these permissions.

The server reconciles workloads every `CAPMESH_WORKLOAD_RECONCILE_INTERVAL`
(5 seconds by default), adding streams for new/recreated Pods and removing old
ones. A disconnected Kubernetes log stream reconnects with bounded backoff and
uses a small time overlap to reduce loss; this favors completeness and can
produce a duplicate line around a reconnect boundary.

The desired job state is persisted in `log-recovery.json`. A replacement server
pod resumes the newest job whose `desired_state` is `RUNNING`, re-resolves its
workloads, and opens new log segments. Recovery requests a one-second overlap
from the Kubernetes log API to reduce loss, so a duplicate line can occur at the
restart boundary. An explicit stop persists `STOPPED` and prevents recovery.
