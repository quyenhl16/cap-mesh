# CapMesh User Guide

Tài liệu này hướng dẫn triển khai CapMesh trên Kubernetes, sinh TLS certificate
và cấu hình client, sử dụng command line, quản lý recording và chạy các tính
năng capture packet/log.

## 1. Tổng quan

CapMesh gồm ba thành phần:

- `capmesh-agent`: chạy dưới dạng DaemonSet trên từng worker node, gọi
  `dumpcap` để bắt packet và gửi packet về server qua gRPC.
- `capmesh-server`: quản lý session, agent, recording, continuous capture và
  workload log capture. Server hiện chạy một replica.
- `capmesh-client`: CLI dùng để tạo, xem, stream, dừng session và quản lý
  recording.

Luồng dữ liệu chính:

```text
dumpcap -> capmesh-agent -> capmesh-server -> capmesh-client -> Wireshark/file
```

Các manifest Kubernetes nằm trong:

```text
deploy/base/
deploy/overlays/dev/
deploy/overlays/production/
```

## 2. Yêu cầu trước khi triển khai

- Kubernetes cluster và quyền chạy `kubectl`.
- Kustomize tích hợp trong `kubectl` (`kubectl apply -k`).
- Docker hoặc công cụ tương thích để build/push image.
- OpenSSL trên máy chạy `deploy/generate.sh`.
- Bash để chạy script generate.
- Một worker node có thư mục lưu recording nếu dùng Local PersistentVolume.
- `capmesh-client` trên máy quản trị.
- Wireshark hoặc `tshark` nếu cần đọc PCAPNG.

Agent image đã cài sẵn `dumpcap`, `iproute2` và CA certificates.

## 3. Build và push image

Thay `REGISTRY` và `VERSION` bằng registry/tag thực tế:

```bash
export REGISTRY=registry.example.com/network
export VERSION=0.0.12

docker build -f Dockerfile.server \
  -t "$REGISTRY/capmesh-server:$VERSION" .

docker build -f Dockerfile.agent \
  -t "$REGISTRY/capmesh-agent:$VERSION" .

docker push "$REGISTRY/capmesh-server:$VERSION"
docker push "$REGISTRY/capmesh-agent:$VERSION"
```

Manifest base đang dùng `capmesh-server:latest` và `capmesh-agent:latest`. Có
thể khai báo image trong overlay production:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base
patches:
  - path: resources.yaml
images:
  - name: capmesh-server
    newName: registry.example.com/network/capmesh-server
    newTag: 0.0.12
  - name: capmesh-agent
    newName: registry.example.com/network/capmesh-agent
    newTag: 0.0.12
```

Nếu registry cần xác thực, tạo `imagePullSecret` và bổ sung vào Pod spec của
Deployment/DaemonSet.

Build client binary trên máy quản trị:

```bash
go build -mod=vendor -trimpath -o bin/capmesh-client ./cmd/capmesh-client
```

Có thể copy `bin/capmesh-client` vào một thư mục thuộc `PATH` để gọi trực tiếp.

## 4. Sinh certificate và client config

### 4.1. Kết quả của script

`deploy/generate.sh` thực hiện:

1. Sinh CA và server certificate.
2. Thêm DNS SAN cho Kubernetes Service.
3. Kiểm tra server certificate bằng CA vừa sinh.
4. Tạo `config.yaml` cho client.
5. Copy `config.yaml` và `ca.crt` vào thư mục cấu hình user.
6. Backup `config.yaml` cũ trước khi ghi file mới.

Các file dành cho Kubernetes được tạo trong thư mục đang chạy script:

```text
ca.crt
server.crt
server.key
```

Client config được cài vào:

- Linux: `${XDG_CONFIG_HOME:-$HOME/.config}/capmesh/config.yaml`
- macOS: `$HOME/Library/Application Support/capmesh/config.yaml`
- Windows/Git Bash: `%APPDATA%/capmesh/config.yaml`
- Hoặc thư mục truyền qua `--config-dir`/`CAPMESH_CONFIG_DIR`.

CA private key chỉ dùng trong lúc sinh certificate và bị xóa khi script hoàn
tất. Hãy bảo vệ `server.key` và không commit certificate/key vào Git.

### 4.2. Các option của `generate.sh`

| Option | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `--server ADDRESS` | `capmesh-server:18443` | Địa chỉ mà client dùng để kết nối server. |
| `--context NAME` | `default` | Tên context được tạo và chọn làm context hiện tại. |
| `--config-dir DIR` | Theo `os.UserConfigDir()` | Thư mục chứa `config.yaml` và bản copy `ca.crt`. |
| `--token-file FILE` | Rỗng | Ghi đường dẫn token file vào context; script không tự tạo token. |
| `--service-name NAME` | `capmesh-server` | Tên Service và TLS server name được ghi vào certificate/config. |
| `--namespace NAME` | `capmesh` | Namespace dùng để tạo DNS SAN Kubernetes. |
| `--valid-days DAYS` | `3650` | Số ngày certificate có hiệu lực. |

`deploy/generate-certs.sh` vẫn tồn tại để tương thích nhưng chỉ chuyển tiếp sang
`generate.sh` và sẽ in cảnh báo deprecated.

### 4.3. Ví dụ dùng port-forward

Đây là cách đơn giản nhất khi client chạy trên máy quản trị:

```bash
mkdir -p "$HOME/.config/capmesh"
printf '%s' 'replace-with-a-strong-token' > "$HOME/.config/capmesh/token"
chmod 600 "$HOME/.config/capmesh/token"

bash ./deploy/generate.sh \
  --server 127.0.0.1:18443 \
  --context production \
  --token-file "$HOME/.config/capmesh/token"
```

Sau khi deploy, mở tunnel:

```bash
kubectl -n capmesh port-forward service/capmesh-server 18443:18443
```

Trong trường hợp này:

- `server` là `127.0.0.1:18443` vì client kết nối tới local port-forward.
- `tlsServerName` vẫn là `capmesh-server` vì certificate có DNS SAN đó.

### 4.4. Ví dụ dùng địa chỉ được expose bên ngoài

Nếu đã expose server qua LoadBalancer, NodePort hoặc TCP ingress:

```bash
bash ./deploy/generate.sh \
  --server 10.10.20.30:18443 \
  --context production \
  --token-file /secure/path/capmesh.token
```

Certificate mặc định vẫn được xác minh bằng `tlsServerName: capmesh-server`.
Nếu muốn xác minh bằng một DNS khác, DNS đó phải có trong SAN của certificate;
cần điều chỉnh script/certificate tương ứng.

## 5. Chuẩn bị Kubernetes

### 5.1. Namespace và Secret

Dùng cùng token đã đặt trong client token file:

```bash
kubectl create namespace capmesh

kubectl -n capmesh create secret generic capmesh-auth \
  --from-literal=token='replace-with-a-strong-token'

kubectl -n capmesh create secret generic capmesh-tls \
  --from-file=tls.crt=server.crt \
  --from-file=tls.key=server.key \
  --from-file=ca.crt=ca.crt
```

Nếu Secret đã tồn tại, có thể cập nhật theo cách declarative:

```bash
kubectl -n capmesh create secret generic capmesh-auth \
  --from-literal=token='replace-with-a-strong-token' \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n capmesh create secret generic capmesh-tls \
  --from-file=tls.crt=server.crt \
  --from-file=tls.key=server.key \
  --from-file=ca.crt=ca.crt \
  --dry-run=client -o yaml | kubectl apply -f -
```

Manifest mặc định dùng một shared token cho server, agent và client. Server cũng
hỗ trợ tách `agent`, `viewer`, `admin` token, nhưng khi dùng cách đó cần bổ sung
các Secret/env tương ứng vào manifest.

### 5.2. Chuẩn bị Local PersistentVolume

Base manifest dùng Local PV:

```text
Host path:       /var/lib/capmesh/captures
Container path:  /app/captures
Capacity:        100Gi
Storage class:   capmesh-local
Access mode:     ReadWriteOnce
Reclaim policy:  Retain
```

Xem hostname các node:

```bash
kubectl get nodes -L kubernetes.io/hostname
```

Thay `REPLACE_WITH_STORAGE_NODE_HOSTNAME` trong
`deploy/base/server-pv.yaml` bằng hostname node lưu trữ. Trên node đó chạy:

```bash
sudo mkdir -p /var/lib/capmesh/captures
sudo chown 65532:65532 /var/lib/capmesh/captures
sudo chmod 0750 /var/lib/capmesh/captures
```

Local PV gắn server vào một node cụ thể. Nếu dùng storage class khác, thay
`server-pv.yaml`/`server-pvc.yaml` bằng PVC phù hợp với hệ thống storage của
cluster.

### 5.3. Khai báo interface cho agent

Agent có thể đọc mapping interface từ Node annotation. Ví dụ:

```bash
kubectl annotate node worker-01 \
  capture.capmesh.io/interface.management=ens192 \
  capture.capmesh.io/interface.data=ens224 \
  capture.capmesh.io/interface.storage=bond0 \
  --overwrite
```

Kiểm tra interface thật trên node trước khi annotate. Mỗi node hỗ trợ tối đa 10
alias. Alias:

- Không phân biệt chữ hoa/chữ thường.
- Dài từ 1 đến 53 ký tự.
- Chỉ gồm chữ cái, chữ số hoặc dấu `-`.
- Phải bắt đầu và kết thúc bằng chữ hoặc số.

`CAPMESH_INTERFACES_FROM_KUBERNETES=true` trong ConfigMap bật cơ chế đọc
annotation. Mapping cấu hình trực tiếp bằng `CAPMESH_INTERFACES` được ưu tiên
hơn annotation cùng alias.

## 6. Cấu hình server và agent

Các giá trị nằm trong `deploy/base/configmap.yaml` và được inject qua
`envFrom`. Flag trực tiếp, nếu được thêm vào container args, có độ ưu tiên cao
hơn environment.

### 6.1. Cấu hình server

| Environment | Flag tương ứng | Mặc định manifest | Ý nghĩa |
| --- | --- | --- | --- |
| `CAPMESH_SERVER_LISTEN` | `--listen` | `:18443` | Địa chỉ gRPC server lắng nghe. |
| `CAPMESH_SERVER_METRICS_LISTEN` | `--metrics-listen` | `:19090` | Endpoint Prometheus metrics của server. |
| `CAPMESH_TLS_CERT` | `--tls-cert` | `/var/run/capmesh/tls/tls.crt` | Server certificate. |
| `CAPMESH_TLS_KEY` | `--tls-key` | `/var/run/capmesh/tls/tls.key` | Server private key. |
| `CAPMESH_SUBSCRIBER_QUEUE_SIZE` | `--subscriber-queue-size` | `10000` | Queue packet riêng cho mỗi subscriber. |
| `CAPMESH_RECORD_DIR` | `--record-dir` | `/app/captures` | Root lưu PCAPNG, log và recovery metadata; rỗng sẽ tắt recording. |
| `CAPMESH_RECORD_SEGMENT_SIZE` | `--record-segment-size` | `10MiB` | Kích thước tối đa mỗi PCAP/log segment; `0` tắt rotation. |
| `CAPMESH_RECORD_MAX_SESSION_SIZE` | `--record-max-session-size` | `10GiB` | Giới hạn mỗi normal session; đồng thời là rolling retention cho continuous packet/log. `0` là không giới hạn. |
| `CAPMESH_RECORD_MAX_NORMAL_TOTAL_SIZE` | `--record-max-normal-total-size` | `50GiB` | Tổng quota các thư mục normal session; `0` tắt quota và cleanup API. |
| `CAPMESH_RECORD_QUEUE_SIZE` | `--record-queue-size` | `65536` | Queue packet/log dùng cho recording. |
| `CAPMESH_WORKLOAD_RECONCILE_INTERVAL` | `--workload-reconcile-interval` | `5s` | Chu kỳ tìm lại Pod/Calico endpoint và reconcile workload. |

Các biến token của server:

| Environment | Quyền |
| --- | --- |
| `CAPMESH_TOKEN` | Shared token, có tất cả quyền; manifest mặc định dùng biến này. |
| `CAPMESH_ADMIN_TOKEN` | Tạo, dừng, xem session và chạy cleanup. |
| `CAPMESH_VIEWER_TOKEN` | Xem metadata/packet, không được thay đổi trạng thái. |
| `CAPMESH_AGENT_TOKEN` | Chỉ dành cho kết nối agent. |

### 6.2. Cấu hình agent

| Environment | Flag tương ứng | Mặc định manifest | Ý nghĩa |
| --- | --- | --- | --- |
| `CAPMESH_SERVER` | `--server` | `capmesh-server:18443` | Địa chỉ gRPC server. |
| `NODE_NAME` | `--node` | Downward API | Kubernetes node mà agent đang chạy. |
| `CAPMESH_TLS_CA` | `--tls-ca` | `/var/run/capmesh/tls/ca.crt` | CA dùng xác minh server. |
| `CAPMESH_TLS_SERVER_NAME` | `--tls-server-name` | `capmesh-server` | DNS SAN dùng khi xác minh TLS. |
| `CAPMESH_INSECURE` | `--insecure` | `false` | Tắt TLS; chỉ dùng cho local development. |
| `CAPMESH_DUMPCAP` | `--dumpcap` | `dumpcap` | Đường dẫn binary dumpcap. |
| `CAPMESH_AGENT_METRICS_LISTEN` | `--metrics-listen` | `:19091` | Endpoint Prometheus metrics của agent. |
| `CAPMESH_CAPTURE_LOG_INTERVAL` | `--capture-log-interval` | `10s` | Chu kỳ log số packet/interface; `0` để tắt. |
| `CAPMESH_INTERFACES_FROM_KUBERNETES` | `--interfaces-from-kubernetes` | `true` | Đọc mapping từ Node annotation. |
| `CAPMESH_INTERFACES` | `--interface` | Rỗng | Danh sách mapping `alias=physical`, có thể lặp lại. |
| `CAPMESH_INTERFACE_A/B/C` | `--interface-a/b/c` | Rỗng | Mapping tương thích cũ cho alias A/B/C. |

DaemonSet dùng `hostNetwork: true` và Linux capabilities `NET_RAW`, `NET_ADMIN`.

## 7. Triển khai và kiểm tra

Triển khai overlay production:

```bash
kubectl apply -k deploy/overlays/production
```

Theo dõi rollout:

```bash
kubectl -n capmesh rollout status deployment/capmesh-server
kubectl -n capmesh rollout status daemonset/capmesh-agent
kubectl -n capmesh get pods -o wide
kubectl -n capmesh get service,pvc
```

Kiểm tra log:

```bash
kubectl -n capmesh logs deployment/capmesh-server
kubectl -n capmesh logs daemonset/capmesh-agent --tail=100
```

Sau khi sửa ConfigMap hoặc Secret, restart workload:

```bash
kubectl -n capmesh rollout restart deployment/capmesh-server
kubectl -n capmesh rollout restart daemonset/capmesh-agent
```

Nếu dùng port-forward:

```bash
kubectl -n capmesh port-forward service/capmesh-server 18443:18443
```

Tại terminal khác, kiểm tra kết nối:

```bash
capmesh-client get agents
```

## 8. Client config

### 8.1. Cấu trúc file

```yaml
apiVersion: capmesh.io/v1
kind: ClientConfig
currentContext: production
contexts:
  production:
    server: 127.0.0.1:18443
    insecure: false
    tlsCA: /home/user/.config/capmesh/ca.crt
    tlsServerName: capmesh-server
    tokenFile: /home/user/.config/capmesh/token
    defaults:
      snaplen: 4096
      ttl: 5m
      reorderWindow: 300ms
      direction: egress
      follow: true
      maxPods: 100
```

Tên trường YAML dùng `lowerCamelCase`; flag tương ứng dùng `kebab-case`, ví dụ
`tlsServerName` tương ứng `--tls-server-name`.

### 8.2. Ý nghĩa các trường

| Trường | Ý nghĩa |
| --- | --- |
| `apiVersion` | Phiên bản schema, hiện phải là `capmesh.io/v1`. |
| `kind` | Loại config, hiện phải là `ClientConfig`. |
| `currentContext` | Context mặc định được client sử dụng. |
| `contexts` | Map chứa một hoặc nhiều context. |
| `contexts.<name>.server` | Địa chỉ host/IP và port mà client kết nối. |
| `contexts.<name>.insecure` | `true` để tắt TLS; không dùng trong production. |
| `contexts.<name>.tlsCA` | Đường dẫn CA certificate. |
| `contexts.<name>.tlsServerName` | Tên phải khớp DNS SAN của server certificate. |
| `contexts.<name>.tokenFile` | File chứa bearer token; an toàn hơn truyền `--token` trên command line. |
| `defaults.snaplen` | Số byte tối đa giữ lại trên mỗi packet. |
| `defaults.ttl` | Thời gian sống mặc định của normal session. |
| `defaults.reorderWindow` | Khoảng thời gian server giữ packet để reorder. |
| `defaults.direction` | Chiều traffic workload: `egress`, `ingress` hoặc `both`. |
| `defaults.follow` | Theo dõi Pod scale/restart/reschedule trong suốt session. |
| `defaults.maxPods` | Số Pod tối đa được phép resolve cho một workload target. |

Client parse config ở chế độ strict; field không hợp lệ hoặc sai tên sẽ gây lỗi.

### 8.3. Thứ tự ưu tiên

```text
CLI flag > environment variable > selected context > built-in default
```

Environment client hỗ trợ:

```text
CAPMESH_SERVER
CAPMESH_INSECURE
CAPMESH_TLS_CA
CAPMESH_TLS_SERVER_NAME
CAPMESH_TOKEN_FILE
CAPMESH_TOKEN
CAPMESH_SNAPLEN
CAPMESH_TTL
CAPMESH_REORDER_WINDOW
CAPMESH_DIRECTION
CAPMESH_FOLLOW
CAPMESH_MAX_PODS
```

### 8.4. Quản lý context

```bash
# Xem config gốc
capmesh-client config view

# Xem giá trị cuối cùng sau khi merge context và environment
capmesh-client config view --effective

# Liệt kê context; dấu * là context hiện tại
capmesh-client config get-contexts

# In tên context hiện tại
capmesh-client config current-context

# Tạo hoặc cập nhật context
capmesh-client config set-context staging \
  --server 127.0.0.1:28443 \
  --tls-ca /secure/staging-ca.crt \
  --tls-server-name capmesh-server \
  --token-file /secure/staging.token \
  --snaplen 4096 \
  --ttl 10m \
  --reorder-window 300ms \
  --direction both \
  --follow=true \
  --max-pods 100

# Chuyển context
capmesh-client config use-context staging
```

Các config subcommand chấp nhận `--config PATH` để thao tác một file khác.
`config view --effective` chấp nhận `--context NAME`.

## 9. Cấu trúc command CLI

Cú pháp chung:

```text
capmesh-client VERB RESOURCE [NAME] [FLAGS]
```

### 9.1. Ý nghĩa verb

| Verb | Ý nghĩa |
| --- | --- |
| `get` | Lấy danh sách hoặc trạng thái hiện tại. |
| `describe` | Lấy một resource cụ thể; output hiện dùng lại format hiện có của `get`. |
| `create` | Tạo normal session rồi thoát, không stream packet. |
| `capture` | Tạo session, stream PCAPNG ra stdout và tự dừng session khi client thoát. |
| `stream` | Stream packet của session đã tồn tại; không tự dừng session đó. |
| `start` | Khởi động singleton continuous packet/log capture. |
| `stop` | Dừng session hoặc singleton capture. |
| `clean` | Xóa recording normal session cũ theo retention rule. |
| `config` | Xem và quản lý client context. |

### 9.2. Danh sách command

| Command | Ý nghĩa |
| --- | --- |
| `get sessions` | Liệt kê normal session đang `RUNNING`. |
| `get session ID` | Lấy một session theo ID. |
| `describe session ID` | Alias chi tiết của `get session ID`. |
| `get agents` | Liệt kê agent đang kết nối và mapping interface. |
| `get agent NODE` | Lấy một agent theo node name. |
| `describe agent NODE` | Alias chi tiết của `get agent NODE`. |
| `get recordings` | Liệt kê thư mục normal recording và dung lượng. |
| `get recording ID` | Tìm recording theo session ID. |
| `describe recording ID` | Alias chi tiết của `get recording ID`. |
| `get continuous` | Xem continuous packet capture. |
| `describe continuous` | Tương đương `get continuous`. |
| `get logcapture` | Xem continuous workload log capture. `log`/`logs` là alias. |
| `describe logcapture` | Tương đương `get logcapture`. |
| `create session` | Tạo normal session rồi trả về session metadata. |
| `capture session` | Tạo session, stream PCAPNG và tự stop khi client thoát. |
| `stream session ID` | Stream một session có sẵn ra stdout. |
| `stop session ID` | Dừng normal session. |
| `start continuous` | Khởi động singleton continuous packet capture. |
| `stop continuous` | Dừng continuous packet capture. |
| `start logcapture` | Khởi động singleton workload log capture. |
| `stop logcapture` | Dừng workload log capture. |
| `clean recordings --dry-run` | Xem trước các thư mục sẽ bị xóa. |
| `clean recordings` | Preview, cảnh báo, yêu cầu xác nhận rồi cleanup. |
| `clean recordings --yes` | Cleanup không hỏi xác nhận, phù hợp automation. |

CLI mode flag cũ vẫn hoạt động trong giai đoạn chuyển đổi nhưng sẽ in cảnh báo
deprecated. Nên dùng command tree mới.

## 10. Các flag của client

### 10.1. Global connection flags

Các flag này có thể đặt trước hoặc sau command và override client config:

| Flag | Ý nghĩa |
| --- | --- |
| `--config PATH` | Dùng client config khác file mặc định. |
| `--context NAME` | Dùng context khác mà không thay `currentContext`. |
| `--server HOST:PORT` | Override địa chỉ server. |
| `--token TOKEN` | Bearer token trực tiếp; có thể lộ qua process list. |
| `--token-file PATH` | Đọc bearer token từ file. |
| `--tls-ca PATH` | CA certificate dùng xác minh server. |
| `--tls-server-name NAME` | TLS server name phải khớp certificate SAN. |
| `--insecure` | Tắt TLS; chỉ dùng local development. |

### 10.2. Session và continuous packet flags

| Flag | Command áp dụng | Ý nghĩa |
| --- | --- | --- |
| `--nodes n1,n2` | `create/capture session`, `start continuous` | Chọn node cho interface target; để rỗng nghĩa là các agent phù hợp. |
| `--interface ALIAS` | Như trên | Logical interface alias; có thể lặp lại. Mặc định `A` nếu không có workload target/interface. |
| `--namespace NS` | Như trên | Namespace workload target. |
| `--workload-kind KIND` | Như trên | `deployment` hoặc `statefulset`. |
| `--workload-name NAME` | Như trên | Tên workload. Ba flag workload phải được truyền cùng nhau. |
| `--direction VALUE` | Như trên | `egress`, `ingress` hoặc `both`. |
| `--follow=true/false` | Như trên | Theo dõi thay đổi Pod trong khi capture. |
| `--max-pods N` | Workload target | Giới hạn số Pod được resolve. |
| `--filter BPF` | Packet capture | BPF filter được chuyển tới dumpcap, ví dụ `tcp port 443`. |
| `--snaplen N` | Packet capture | Số byte tối đa giữ lại mỗi packet; `65535` khi cần payload đầy đủ. |
| `--ttl DURATION` | Normal session | Thời gian sống của session, ví dụ `30s`, `5m`, `1h`. |
| `--reorder-window DURATION` | Packet capture | Cửa sổ reorder packet, mặc định `300ms`. |

### 10.3. Workload log flags

| Flag | Ý nghĩa |
| --- | --- |
| `--workload NS/KIND/NAME` | Workload cần lấy log; KIND là `deployment` hoặc `statefulset`; có thể lặp lại. |
| `--container NAME` | Container cần lấy log; có thể lặp lại. Bỏ qua để lấy mọi regular container. |
| `--log-since DURATION` | Lấy thêm log trong khoảng thời gian trước lúc start; `0` chỉ lấy log mới. |
| `--max-pods N` | Giới hạn Pod của mỗi workload target. |

Các alias cũ `--log-workload` và `--log-container` vẫn được hỗ trợ.

### 10.4. Cleanup flags

| Flag | Ý nghĩa |
| --- | --- |
| `--dry-run` | Chỉ hiển thị kế hoạch xóa, không thay đổi filesystem. |
| `--yes` | Bỏ prompt xác nhận. Chỉ hợp lệ với `clean recordings`. |

## 11. Ví dụ sử dụng

### 11.1. Kiểm tra agent

```bash
capmesh-client get agents
capmesh-client describe agent worker-01
```

### 11.2. Tạo normal session không stream

Capture interface `management` trên hai node trong 10 phút:

```bash
capmesh-client create session \
  --nodes worker-01,worker-02 \
  --interface management \
  --filter "tcp port 443" \
  --snaplen 4096 \
  --ttl 10m
```

Xem và dừng session:

```bash
capmesh-client get sessions
capmesh-client describe session SESSION_ID
capmesh-client stop session SESSION_ID
```

### 11.3. Capture trực tiếp ra Wireshark

```bash
capmesh-client capture session \
  --nodes worker-01 \
  --interface management \
  --filter "tcp port 443" \
| wireshark -k -i -
```

`capture session` tự stop session khi client kết thúc hoặc nhận Ctrl+C.

### 11.4. Capture ra file PCAPNG

```bash
capmesh-client capture session \
  --nodes worker-01 \
  --interface data \
  --ttl 5m \
> capture.pcapng

tshark -r capture.pcapng
```

CLI ghi PCAPNG vào stdout, log và warning vào stderr nên redirect trên là an
toàn.

### 11.5. Stream session đang tồn tại

```bash
capmesh-client stream session SESSION_ID > existing-session.pcapng
```

Việc dừng stream không dừng session đang tồn tại.

### 11.6. Capture theo Kubernetes workload

```bash
capmesh-client create session \
  --namespace payment \
  --workload-kind deployment \
  --workload-name payment-api \
  --direction both \
  --follow=true \
  --max-pods 50 \
  --filter "tcp port 443" \
  --ttl 15m
```

Server dùng Pod/ReplicaSet/Deployment/StatefulSet và Calico WorkloadEndpoint để
resolve Pod về node/interface.

### 11.7. Continuous packet capture

```bash
capmesh-client start continuous \
  --nodes worker-01,worker-02 \
  --interface data \
  --filter "not port 18443"

capmesh-client get continuous
capmesh-client stop continuous
```

Continuous capture là singleton. Server ghi rolling PCAPNG vào:

```text
<record-dir>/continuous/
```

Khi vượt `CAPMESH_RECORD_MAX_SESSION_SIZE`, các segment hoàn tất cũ nhất bị xóa
trước. Desired state được lưu trong recovery metadata và job `RUNNING` được
phục hồi khi server pod restart.

### 11.8. Continuous workload log capture

```bash
capmesh-client start logcapture \
  --workload payment/deployment/payment-api \
  --workload payment/statefulset/payment-worker \
  --container app \
  --log-since 5m \
  --max-pods 100

capmesh-client get logcapture
capmesh-client stop logcapture
```

Bỏ `--container` để lấy log của tất cả regular container. Log được lưu dưới:

```text
<record-dir>/logs/continuous/<run-id>/
```

Log capture có rolling retention riêng và cũng được phục hồi từ recovery
metadata nếu desired state vẫn là `RUNNING`.

### 11.9. Quản lý normal recording

Liệt kê thư mục và dung lượng:

```bash
capmesh-client get recordings
capmesh-client describe recording SESSION_ID
```

Xem kế hoạch cleanup:

```bash
capmesh-client clean recordings --dry-run
```

Sau khi backup dữ liệu cần giữ, chạy cleanup:

```bash
capmesh-client clean recordings
```

Automation có thể dùng:

```bash
capmesh-client clean recordings --yes
```

Cleanup xóa nguyên thư mục normal session cũ nhất cho đến khi tổng dung lượng
không vượt quá 50% `CAPMESH_RECORD_MAX_NORMAL_TOTAL_SIZE`. Cleanup không xóa:

- Session đang active.
- Session có recovery desired state `RUNNING`.
- Directory có metadata lỗi/không xác định.
- Symlink hoặc path không an toàn.
- Continuous packet capture và workload log capture.

Khi tổng normal recording đạt giới hạn, `create session`/`capture session` bị từ
chối với `ResourceExhausted`. Hãy backup trước khi cleanup.

## 12. Recovery khi server pod restart

Server ghi recovery metadata trên PersistentVolume. Khi pod mới khởi động:

- Normal/continuous packet session có desired state `RUNNING` được dựng lại.
- Continuous workload log job `RUNNING` mới nhất được dựng lại.
- Agent reconnect sẽ nhận lại capture command.
- Segment `.part` cũ được xử lý và segment mới được mở.

Packet phát sinh trong thời gian server unavailable vẫn có thể mất vì agent
chưa có acknowledged replay buffer. Server hiện phải giữ `replicas: 1`; recovery
giúp pod replacement chứ không tạo active-active HA.

## 13. Metrics

- Server: port `19090`, path `/metrics`.
- Agent: port `19091`, path `/metrics`.

Ví dụ port-forward metrics server:

```bash
kubectl -n capmesh port-forward deployment/capmesh-server 19090:19090
curl http://127.0.0.1:19090/metrics
```

Metrics gồm packet received/emitted/late, reorder buffer, subscriber drop,
recording size/segment, cleanup và log capture.

## 14. Phân quyền

Tóm tắt quyền client:

| Thao tác | Viewer | Admin | Shared |
| --- | --- | --- | --- |
| Xem session/agent/recording/status | Có | Có | Có |
| Stream packet | Có | Có | Có |
| Tạo/dừng session | Không | Có | Có |
| Start/stop continuous hoặc logcapture | Không | Có | Có |
| Cleanup recording | Không | Có | Có |

Không truyền token bằng `--token` nếu không cần thiết. Ưu tiên `tokenFile` với
permission chỉ cho user hiện tại.

## 15. Troubleshooting

### Client báo lỗi xác minh TLS

Kiểm tra:

```bash
capmesh-client config view --effective
openssl x509 -in server.crt -noout -subject -issuer -ext subjectAltName
```

`server` là địa chỉ kết nối thực tế; `tlsServerName` phải khớp DNS SAN trong
certificate.

### Không thấy agent

```bash
kubectl -n capmesh get pods -l app.kubernetes.io/name=capmesh-agent -o wide
kubectl -n capmesh logs daemonset/capmesh-agent --tail=200
kubectl get node worker-01 -o jsonpath='{.metadata.annotations}'
```

Kiểm tra token, CA, DNS Service và interface annotation.

### Agent báo interface không tồn tại

Đăng nhập node và kiểm tra:

```bash
ip link
```

Sau đó sửa annotation và restart DaemonSet nếu cần.

### Server pod Pending

Kiểm tra PV/PVC và node affinity:

```bash
kubectl -n capmesh describe pod -l app.kubernetes.io/name=capmesh-server
kubectl get pv capmesh-captures-local
kubectl -n capmesh describe pvc capmesh-captures
```

Đảm bảo hostname trong `server-pv.yaml` đúng và thư mục host đã được tạo.

### Server không ghi được recording

Kiểm tra mount và permission:

```bash
kubectl -n capmesh describe pod -l app.kubernetes.io/name=capmesh-server
kubectl -n capmesh logs deployment/capmesh-server
```

Thư mục host cần cho UID/GID của container (`65532`) ghi dữ liệu.

### Đã sửa ConfigMap nhưng Pod vẫn dùng giá trị cũ

Environment chỉ được nạp khi Pod khởi động:

```bash
kubectl -n capmesh rollout restart deployment/capmesh-server
kubectl -n capmesh rollout restart daemonset/capmesh-agent
```

### Cleanup không đạt target 50%

Các recording còn lại có thể đang active, còn khả năng recovery hoặc metadata
không an toàn để xóa. Chạy:

```bash
capmesh-client get recordings
capmesh-client clean recordings --dry-run
```

Kiểm tra các trường `active`, `desired_state` và `deletable` trong output.
