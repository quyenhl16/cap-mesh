# CapMesh

CapMesh là hệ thống bắt và xem packet phân tán gần thời gian thực trên Kubernetes. Agent chạy trên từng worker, điều khiển `dumpcap`, chuyển packet qua gRPC về server; client hợp nhất các nguồn thành PCAPNG nhiều interface để Wireshark đọc trực tiếp từ `stdin`.

```text
dumpcap -> capmesh-agent ==gRPC==> capmesh-server ==gRPC==> capmesh-client -> Wireshark
```

## Trạng thái MVP

Repository này cung cấp ba chương trình:

- `capmesh-agent`: ánh xạ interface logic A/B/C, quản lý `dumpcap`, batch tối đa 64 packet hoặc 10 ms và tự kết nối lại server.
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
go run ./cmd/capmesh-server --token dev-secret
```

Terminal 2 — agent (đổi `eth0` thành interface thật):

```bash
go run ./cmd/capmesh-agent \
  --server 127.0.0.1:18443 \
  --node worker-local \
  --interface-a eth0 \
  --token dev-secret \
  --insecure
```

Terminal 3 — tạo session và xem ngay bằng Wireshark:

```bash
go run ./cmd/capmesh-client \
  --server 127.0.0.1:18443 \
  --create \
  --nodes worker-local \
  --interface A \
  --filter "tcp port 443" \
  --snaplen 256 \
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
capmesh-agent --server capture.example.com:18443 --tls-ca ca.crt --interface-a eth0
capmesh-client --server capture.example.com:18443 --session SESSION_ID --tls-ca ca.crt | wireshark -k -i -
```

Không truyền token trên command line nếu có thể; dùng biến môi trường `CAPMESH_TOKEN` để tránh lộ qua process list.

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
  capture.capmesh.io/interface-a=ens192 \
  capture.capmesh.io/interface-b=ens224 \
  capture.capmesh.io/interface-c=bond0
```

Agent đọc annotation qua Kubernetes API bằng quyền tối thiểu `get nodes`. Triển khai:

```bash
kubectl apply -k deploy/overlays/production
```

DaemonSet dùng `hostNetwork` và chỉ thêm `NET_RAW`, `NET_ADMIN`; các Linux capability còn lại bị drop.

## Cấu hình chính

| Thành phần | Flag | Mặc định |
| --- | --- | --- |
| Server | `--subscriber-queue-size` | `10000` |
| Agent | batch packet / delay | `64` / `10ms` (MVP cố định) |
| Client | `--reorder-window` | `300ms` |
| Client | flush packet / delay | `64` / `50ms` (MVP cố định) |
| Client | `--ttl` | `5m` |

Nếu queue của một subscriber đầy, server drop packet chỉ trên subscriber đó; capture và các subscriber khác không bị backpressure. Counter `capmesh_server_subscriber_drops_total` ghi nhận tình trạng này.

## Metrics

- Server: `:19090/metrics`.
- Agent: `:9091/metrics`.

Các metric chính gồm packet received/emitted/late, reorder buffer size, subscriber drops/queue usage, agent captured/sent bytes và stream errors.

## Giới hạn MVP

- State chỉ ở memory, không HA và không replay/ring buffer.
- Đồng hồ worker phải đồng bộ bằng NTP/Chrony; reorder không sửa clock skew.
- BPF được truyền dưới dạng một argument riêng, không qua shell; `dumpcap` trên agent là nơi compile và từ chối filter không hợp lệ.
- Capture drop do kernel/`dumpcap` chưa được parse từ thống kê cuối phiên; metric được đăng ký nhưng chỉ tăng khi adapter capture bổ sung nguồn thống kê tương ứng.
- Chưa có lưu trữ object, web UI, extcap, deduplicate hoặc capture engine AF_PACKET/eBPF.
