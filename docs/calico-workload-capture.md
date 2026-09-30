# Calico workload capture

CapMesh can capture multiple configured interface aliases and all Pods owned by
one StatefulSet or Deployment in the same session.

## Multiple configured interfaces

Repeat `--interface`. `--nodes` applies only to these interface targets.

```bash
capmesh-client --create \
  --nodes worker-01,worker-02 \
  --interface uplink \
  --interface data
```

## Workload capture

The server reads the workload selector, verifies the Pod owner chain, and maps
each Pod to the host-side Calico interface through the Calico
WorkloadEndpoint. The agent falls back to `ip route get <pod-ip>` when the
WorkloadEndpoint does not yet contain an interface name.

```bash
capmesh-client --create \
  --namespace payment \
  --workload-kind statefulset \
  --workload-name payment-worker \
  --direction egress \
  --follow=true \
  --max-pods 100
```

Supported directions are `egress`, `ingress`, and `both`. The server combines
the selected direction with the optional user filter. For example, egress with
`--filter "tcp port 443"` becomes:

```text
(src host <pod-ip>) and (tcp port 443)
```

## Combined capture

```bash
capmesh-client --create \
  --nodes worker-01,worker-02 \
  --interface uplink \
  --interface data \
  --namespace payment \
  --workload-kind deployment \
  --workload-name payment-api \
  --direction egress \
  --filter "tcp port 443" \
  --snaplen 65535 \
  --ttl 30m
```

All sources are streamed and recorded as one session. PCAPNG interface names
include the node and either the logical alias or Pod identity. Packets observed
both on a Pod interface and an uplink are intentionally preserved; phase 1 does
not deduplicate observations from different capture points.

## Kubernetes permissions

The server service account needs read access to Pods, Deployments, StatefulSets,
ReplicaSets, and Calico WorkloadEndpoints. The base manifests include this
RBAC. Workload membership is reconciled every five seconds by default; change
`CAPMESH_WORKLOAD_RECONCILE_INTERVAL` to adjust it.

Phase 1 limits a session to 100 workload Pods in total and 50 active capture
sources on one node. This bounds the number of `dumpcap` processes until the
multi-interface capture engine planned for phase 2 is implemented.
