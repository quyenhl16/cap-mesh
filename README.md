# CapMesh

CapMesh là hệ thống bắt và xem packet phân tán gần thời gian thực trên Kubernetes. Agent chạy trên từng worker, điều khiển `dumpcap`, chuyển packet qua gRPC về server; client hợp nhất các nguồn thành PCAPNG nhiều interface để Wireshark đọc trực tiếp từ `stdin`.

```text
dumpcap -> capmesh-agent ==gRPC==> capmesh-server ==gRPC==> capmesh-client -> Wireshark
```

## Trạng thái MVP

Repository này cung cấp ba chương trình:

- `capmesh-agent`: ánh xạ tối đa 10 interface bằng alias tùy chỉnh, quản lý `dumpcap`, batch tối đa 64 packet hoặc 10 ms và tự kết nối lại server.
- `capmesh-server`: quản lý session in-memory, kiểm tra sequence gap, reorder bằng min-heap, TTL tự dừng, fan-out queue độc lập cho từng subscriber và Prometheus metrics.
- `capmesh-client`: tạo hoặc subscribe session, ánh xạ `node/interface` sang PCAPNG Interface ID và chỉ ghi binary PCAPNG ra `stdout`.

TLS 1.2+ và bearer token theo vai trò được hỗ trợ. Chế độ `--insecure` chỉ dành cho phát triển local. Session mất khi server restart và server chỉ chạy một replica, đúng phạm vi MVP.

## Kiến trúc code

```text
api/capmesh/v1/             protobuf và mã gRPC sinh tự động
cmd/                        composition root của ba binary
internal/core/domain/       entity và quy tắc domain thuần Go
internal/core/ports/        interface đi vào/đi ra của application
internal/application/       use case session, agent batching, reorder/fan-out
internal/adapter/            gRPC, dumpcap, memory, PCAPNG, metrics, Kubernetes
deploy/                     Kustomize base và overlays
```

Dependency luôn hướng vào trong: domain không import framework; application chỉ phụ thuộc port; adapter hiện thực port; `cmd` lắp ghép dependency. Vì vậy capture engine, storage hoặc transport có thể thay thế mà không đổi use case.

## Yêu cầu

- Go 1.24.x.
- `dumpcap` trên máy chạy agent.
- Wireshark hoặc `tshark` trên máy chạy client.
- `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc` chỉ cần khi sửa file `.proto`.

## Build và kiểm thử

```bash
go test ./...
go vet ./...
go build ./cmd/...
```

Sinh lại protobuf:

```bash
make generate
```

Đồng bộ dependency vào `vendor` sau mỗi lần thay đổi `go.mod` hoặc `go.sum`:

```bash
go mod tidy
make vendor
```

Docker build sử dụng `go build -mod=vendor` và không chạy `go mod download` bên trong build container. Vì vậy thư mục `vendor/` phải được commit và có mặt trong Docker build context.

## Chạy local

Terminal 1 — server không TLS cho local:

```bash
go run ./cmd/capmesh-server \
  --token dev-secret \
  --record-dir ./data/captures \
  --record-segment-size 10MiB \
  --record-max-session-size 10GiB
```

Khi `--record-dir` được cấu hình, server vừa stream packet cho các client vừa ghi
PCAPNG. Mỗi session nằm trong thư mục `<record-dir>/<YYYY-MM-DD>/<session-id>/`.
File đang ghi có đuôi `.part`; khi đạt giới hạn segment hoặc session kết thúc, file
được flush và đổi tên thành `capture-000001.pcapng`, `capture-000002.pcapng`, ...
`metadata.json` chứa trạng thái, tổng packet/byte và danh sách segment. Để trống
`--record-dir` sẽ tắt chức năng ghi file.

Terminal 2 — agent (đổi `eth0` thành interface thật):

```bash
go run ./cmd/capmesh-agent \
  --server 127.0.0.1:18443 \
  --node worker-local \
  --interface management=eth0 \
  --token dev-secret \
  --insecure
```

Terminal 3 — tạo session và xem ngay bằng Wireshark:

```bash
go run ./cmd/capmesh-client \
  --server 127.0.0.1:18443 \
  --create \
  --nodes worker-local \
  --interface management \
  --filter "tcp port 443" \
  --snaplen 4096 \
  --ttl 5m \
  --token dev-secret \
  --insecure \
| wireshark -k -i -
```

Có thể ghi file thay vì mở Wireshark:

```bash
capmesh-client --server 127.0.0.1:18443 --session SESSION_ID --token dev-secret --insecure > capture.pcapng
tshark -r capture.pcapng
```

Mọi log của client đi tới `stderr`; `stdout` chỉ chứa PCAPNG.

## TLS

Server nhận `--tls-cert` và `--tls-key`. Agent/client dùng trust store hệ thống hoặc `--tls-ca`; `--tls-server-name` dùng khi tên trong certificate khác hostname kết nối.

```bash
capmesh-server --tls-cert server.crt --tls-key server.key --token "$CAPMESH_TOKEN"
capmesh-agent --server capture.example.com:18443 --tls-ca ca.crt --interface management=eth0
capmesh-client \
  --server capture.example.com:18443 \
  --session SESSION_ID \
  --tls-ca ./ca.crt \
  --tls-server-name capture.example.com \
  --token "$CAPMESH_TOKEN" \
| wireshark -k -i -
```

Không truyền token trên command line nếu có thể; dùng biến môi trường `CAPMESH_TOKEN` để tránh lộ qua process list.

Ví dụ đầy đủ khi client kết nối tới Kubernetes Service bằng ClusterIP,
trong khi certificate có DNS SAN là `capmesh-server`:

```bash
export CAPMESH_TOKEN='replace-me'

./bin/capmesh-client \
  --server 10.106.142.184:18443 \
  --tls-ca ./ca.crt \
  --tls-server-name capmesh-server \
  --token "$CAPMESH_TOKEN" \
  --create \
  --namespace pramf01 \
  --workload-kind statefulset \
  --workload-name mm \
  --direction both \
  --follow=true \
  --max-pods 100 \
| wireshark -k -i -
```

`--server` là địa chỉ thực tế dùng để kết nối. `--tls-server-name` phải
trùng chính xác với một DNS SAN trong certificate, ví dụ
`capmesh-server` hoặc `capmesh-server.capmesh.svc`. Nếu kết nối bằng IP mà
không truyền `--tls-server-name`, certificate phải chứa chính IP đó trong IP SAN.

Liệt kê các agent đang kết nối, node và toàn bộ ánh xạ alias/interface:

```bash
./bin/capmesh-client \
  --server 10.106.142.184:18443 \
  --tls-ca ./ca.crt \
  --tls-server-name capmesh-server \
  --token "$CAPMESH_TOKEN" \
  --list-agents
```

Ví dụ output:

```text
agents=1
node=worker-01 status=CONNECTED connected_at=2026-10-01T10:00:00Z last_seen_at=2026-10-01T10:00:10Z interfaces="data=ens224,management=ens192"
```

Danh sách chỉ gồm agent đang có gRPC stream tới server; agent bị ngắt kết nối
sẽ được loại khỏi kết quả. Viewer, admin và shared token đều có quyền gọi API này.

Liệt kê các session thông thường đang chạy:

```bash
./bin/capmesh-client \
  --server 10.106.142.184:18443 \
  --tls-ca ./ca.crt \
  --tls-server-name capmesh-server \
  --token "$CAPMESH_TOKEN" \
  --list-sessions
```

Ví dụ output:

```text
sessions=1
session_id=session-abcd mode=NORMAL status=RUNNING nodes="worker-01" created_at=2026-10-01T10:00:00Z expires_at=2026-10-01T10:05:00Z targets="workload:pramf01/statefulset/mm(both)" filter="" message=""
```

API `ListSessions` hỗ trợ lọc theo `status` và `mode`; CLI `--list-sessions`
mặc định yêu cầu `RUNNING/NORMAL`. Danh sách nằm trong memory và sẽ mất khi
server restart. Viewer, admin và shared token đều có quyền xem.

## Continuous workload log capture

Server có thể chạy một singleton background job để ghi nguyên văn log của tất
cả Pod thuộc một hoặc nhiều StatefulSet/Deployment. Client chỉ gọi start rồi có
thể thoát; job chỉ dừng khi gọi `--log-stop` hoặc server shutdown. Singleton log
độc lập với singleton packet capture nên hai loại có thể chạy đồng thời.

```bash
export CAPMESH_TOKEN='replace-me'

./bin/capmesh-client \
  --server 10.106.142.184:18443 \
  --tls-ca ./ca.crt \
  --tls-server-name capmesh-server \
  --token "$CAPMESH_TOKEN" \
  --log-start \
  --log-workload pramf01/statefulset/mm \
  --log-workload pramf01/deployment/api \
  --log-container app \
  --log-since 5m \
  --max-pods 100

./bin/capmesh-client --server 10.106.142.184:18443 --tls-ca ./ca.crt \
  --tls-server-name capmesh-server --token "$CAPMESH_TOKEN" --log-status

./bin/capmesh-client --server 10.106.142.184:18443 --tls-ca ./ca.crt \
  --tls-server-name capmesh-server --token "$CAPMESH_TOKEN" --log-stop
```

Bỏ `--log-container` để lấy mọi application container. `--log-since 0` chỉ lấy
log mới từ thời điểm start. File nằm dưới
`<record-dir>/logs/continuous/<run-id>/`; mỗi Pod UID, container và restart count
có thư mục riêng. Nội dung `.log` giống `kubectl logs ... > file.log`, không bị
đóng gói JSON hoặc thêm prefix. Rotate dùng `CAPMESH_RECORD_SEGMENT_SIZE`, còn
quota rolling toàn bộ log dùng `CAPMESH_RECORD_MAX_SESSION_SIZE`.

Chi tiết API, cấu trúc file, retention và hành vi reconnect xem tại
[`docs/workload-log-capture.md`](docs/workload-log-capture.md).

`--token` là token dùng chung tương thích cấu hình đơn giản. Khi cần tách quyền, server hỗ trợ `--agent-token`, `--viewer-token`, `--admin-token` (hoặc các biến `CAPMESH_AGENT_TOKEN`, `CAPMESH_VIEWER_TOKEN`, `CAPMESH_ADMIN_TOKEN`). Agent chỉ được mở stream agent; viewer chỉ được xem metadata/packet; admin được tạo, xem và dừng session.

## Triển khai Kubernetes

Build và push hai image, rồi đổi tên/tag trong overlay phù hợp. Base yêu cầu hai Secret trong namespace `capmesh`:

- `capmesh-auth`, key `token`.
- `capmesh-tls`, keys `tls.crt`, `tls.key`, `ca.crt`. Certificate cần SAN cho `capmesh-server` hoặc `capmesh-server.capmesh.svc`.

Ví dụ tạo secret:

```bash
kubectl create namespace capmesh
kubectl -n capmesh create secret generic capmesh-auth --from-literal=token='replace-me'
kubectl -n capmesh create secret generic capmesh-tls \
  --from-file=tls.crt=server.crt \
  --from-file=tls.key=server.key \
  --from-file=ca.crt=ca.crt
```

Gắn ánh xạ interface riêng cho từng worker:

```bash
kubectl annotate node worker-01 \
  capture.capmesh.io/interface.management=ens192 \
  capture.capmesh.io/interface.data=ens224 \
  capture.capmesh.io/interface.storage=bond0
```

Mỗi node hỗ trợ tối đa 10 ánh xạ. Alias không phân biệt hoa thường, dài 1-53 ký tự,
chỉ gồm chữ cái, chữ số hoặc dấu gạch ngang, và phải bắt đầu/kết thúc bằng chữ hoặc số.
CLI có thể lặp lại `--interface alias=physical`; cấu hình CLI được ưu tiên hơn annotation
cùng alias. Các cờ `--interface-a/b/c` và annotation `interface-a/b/c` cũ vẫn được hỗ trợ.

Agent đọc annotation qua Kubernetes API bằng quyền tối thiểu `get nodes`. Triển khai:

```bash
kubectl apply -k deploy/overlays/production
```

DaemonSet dùng `hostNetwork` và chỉ thêm `NET_RAW`, `NET_ADMIN`; các Linux capability còn lại bị drop.

Các tham số khởi động của server và agent nằm trong `deploy/base/configmap.yaml` và
được inject qua `envFrom`. Nếu một key không tồn tại, binary dùng default tích hợp;
flag truyền trực tiếp vẫn ưu tiên hơn giá trị từ ConfigMap. Token tiếp tục lấy từ
Secret, còn `NODE_NAME` lấy từ Downward API. Sau khi sửa ConfigMap, restart workload
để Pod nhận environment mới:

```bash
kubectl -n capmesh rollout restart deployment/capmesh-server
kubectl -n capmesh rollout restart daemonset/capmesh-agent
```

Base manifest dùng static Local PersistentVolume `capmesh-captures-local`, lấy thư
mục `/var/lib/capmesh/captures` trên một node và mount vào container tại
`/app/captures`. Trước khi deploy, xem Kubernetes hostname của các node:

```bash
kubectl get nodes -L kubernetes.io/hostname
```

Thay `REPLACE_WITH_STORAGE_NODE_HOSTNAME` trong `deploy/base/server-pv.yaml` bằng
hostname của node lưu dữ liệu. Sau đó chạy trực tiếp trên node đó:

```bash
sudo mkdir -p /var/lib/capmesh/captures
sudo chown 65532:65532 /var/lib/capmesh/captures
sudo chmod 0750 /var/lib/capmesh/captures
```

PV dùng `nodeAffinity` với key chuẩn `kubernetes.io/hostname`; Kubernetes scheduler
sẽ tự đặt server Pod lên node chứa volume, không cần gán custom label hoặc khai báo
`nodeSelector` trong Deployment. PV/PVC dùng class `capmesh-local`, dung lượng khai
báo `100Gi` và reclaim policy `Retain`.

Recording được bật trong ConfigMap:

```yaml
# ConfigMap data
data:
  CAPMESH_RECORD_DIR: /app/captures
  CAPMESH_RECORD_SEGMENT_SIZE: 10MiB
  CAPMESH_RECORD_MAX_SESSION_SIZE: 10GiB
  CAPMESH_RECORD_QUEUE_SIZE: "65536"

# Server container/pod
volumeMounts:
  - name: captures
    mountPath: /app/captures
volumes:
  - name: captures
    persistentVolumeClaim:
      claimName: capmesh-captures
```

Server phải có quyền ghi vào volume. Không nên dùng `emptyDir` cho capture cần giữ
lại sau khi Pod restart. Pod dùng `fsGroup: 65532` để process non-root trong image
server có quyền ghi. Local PV không tự áp quota vào filesystem; giá trị `100Gi` dùng
cho scheduling/binding, còn dung lượng thực phụ thuộc ổ đĩa của node.

## Cấu hình chính

| Thành phần | Flag | Mặc định |
| --- | --- | --- |
| Server | `--subscriber-queue-size` | `10000` |
| Server | `--record-dir` | Rỗng, recording bị tắt |
| Server | `--record-segment-size` | `10MiB`; `0` để không chia segment |
| Server | `--record-max-session-size` | `10GiB`; `0` để không giới hạn tổng mỗi session |
| Server | `--record-queue-size` | `65536` packet mỗi session |
| Agent | `--interface alias=physical` | Bắt buộc ít nhất một ánh xạ, lặp lại tối đa 10 lần |
| Agent | batch packet / delay | `64` / `10ms` (MVP cố định) |
| Agent | `--capture-log-interval` | `10s`; đặt `0` để tắt log định kỳ |
| Client | `--snaplen` | `4096` byte; dùng `65535` khi cần toàn bộ payload |
| Client | `--reorder-window` | `300ms` |
| Client | flush packet / delay | `64` / `50ms` (MVP cố định) |
| Client | `--ttl` | `5m` |

Nếu queue của một subscriber đầy, server drop packet chỉ trên subscriber đó; capture và các subscriber khác không bị backpressure. Nếu queue recorder đầy, file được finalize dưới dạng `*.partial.pcapng`, metadata chuyển thành `PARTIAL`, còn live stream tiếp tục. Counter `capmesh_server_subscriber_drops_total` ghi nhận tình trạng này.

## Metrics

- Server: `:19090/metrics`.
- Agent: `:19091/metrics`.

Các metric chính gồm packet received/emitted/late, reorder buffer size, subscriber drops/queue usage, agent captured/sent bytes và stream errors. Khi recording được bật, server xuất thêm `capmesh_recording_active`, số packet/byte, số segment, queue overflow và kết quả `COMPLETED/PARTIAL/TRUNCATED/FAILED`.

## Continuous capture

Server hỗ trợ một tiến trình ghi liên tục singleton. Client khởi động xong có thể
thoát; server vẫn capture, chia segment và xóa file hoàn tất cũ nhất khi đạt
`--record-max-session-size`. Xem [hướng dẫn continuous capture](docs/continuous-capture.md)
cho các lệnh start/status/stop, cách stream song song và layout file.

## Giới hạn MVP

- State chỉ ở memory, không HA; continuous capture có retention theo file nhưng chưa hỗ trợ replay packet cũ qua API.
- Đồng hồ worker phải đồng bộ bằng NTP/Chrony; reorder không sửa clock skew.
- BPF được truyền dưới dạng một argument riêng, không qua shell; `dumpcap` trên agent là nơi compile và từ chối filter không hợp lệ.
- Capture drop do kernel/`dumpcap` chưa được parse từ thống kê cuối phiên; metric được đăng ký nhưng chỉ tăng khi adapter capture bổ sung nguồn thống kê tương ứng.
- Chưa có lưu trữ object, web UI, extcap, deduplicate hoặc capture engine AF_PACKET/eBPF.
