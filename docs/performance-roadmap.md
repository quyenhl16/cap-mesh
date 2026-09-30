# Kế hoạch phát triển hiệu năng CapMesh

## Mục tiêu

Nâng throughput và giảm packet loss/latency theo từng bước, đồng thời giữ kiến trúc
dễ vận hành. Mọi tối ưu phải được đo bằng benchmark trước và sau thay đổi; không chuyển
ngay sang eBPF hoặc kiến trúc phân tán khi chưa xác định nút thắt thực tế.

Các chỉ số mục tiêu cần được chốt trước khi triển khai:

- Packet/giây và Gbit/giây trên mỗi agent.
- Tổng throughput một server phải xử lý.
- Số node, session và subscriber đồng thời.
- Tỷ lệ packet loss tối đa chấp nhận được.
- Độ trễ từ lúc bắt packet tới Wireshark.
- Tốc độ ghi và dung lượng lưu trữ yêu cầu.

## Nút thắt của kiến trúc hiện tại

| Thành phần | Nút thắt tiềm năng |
| --- | --- |
| Capture | `dumpcap` tạo PCAP qua stdout, sau đó agent parse lại |
| Agent batching | Batch cố định 64 packet hoặc 10 ms |
| Protocol | Protobuf tạo một object và payload riêng cho từng packet |
| Reorder | Min-heap xử lý từng packet với độ phức tạp `O(log n)` |
| Fan-out | Payload được clone cho từng subscriber |
| Recorder | Ghi và `fsync` có thể tạo pause khi storage chậm |
| Server | Registry và session in-memory, chỉ chạy một replica |
| Storage | Local PV pin server vào một Kubernetes node |

## Nguyên tắc phát triển

- Tối ưu data path hiện tại trước khi thay toàn bộ kiến trúc.
- Không để disk hoặc một subscriber chậm chặn capture pipeline.
- Dùng bounded queue và công khai chính sách drop/backpressure.
- Giữ `dumpcap` làm fallback khi bổ sung capture engine mới.
- Control plane và data plane có thể phát triển độc lập.
- Packet không được ghi qua database; database chỉ lưu metadata/control state.
- Mỗi giai đoạn phải có benchmark, metric và phương án rollback.

## Giai đoạn 0 — Benchmark và quan sát

### Công việc

- Xây dựng traffic replay cố định cho các kích thước packet phổ biến.
- Đo riêng agent, network transport, reorder, fan-out và recorder.
- Bổ sung `pprof`, runtime metrics và histogram latency theo từng stage.
- Ghi nhận allocation/packet, GC pause, CPU, memory và queue utilization.
- Đo kernel drop, agent drop, subscriber drop và recording queue overflow.

### Tiêu chí hoàn thành

- Có benchmark có thể lặp lại trên cùng phần cứng.
- Xác định được stage tiêu thụ CPU/allocation lớn nhất.
- Có dashboard thể hiện throughput, latency và packet loss.

## Giai đoạn 1 — Tối ưu pipeline hiện tại

### 1. Rebatch sau reorder

Thay vì phát từng packet riêng lẻ, gom packet đã reorder thành batch, ví dụ:

```text
Reorder heap → tối đa 256 packet hoặc 1 ms → fan-out
```

Batch size nên cấu hình được và có thể điều chỉnh dựa trên latency mục tiêu.

### 2. Shared immutable batch

Recorder và các subscriber cùng tham chiếu một batch immutable, không clone
`packet.Data` cho từng consumer. Chỉ dùng buffer pool khi vòng đời ownership đã rõ
ràng và có race test.

### 3. Single-owner event loop cho mỗi session

Mỗi session có một goroutine sở hữu reorder state và subscriber map:

```text
publish channel
      ↓
session event loop
  - reorder
  - rebatch
  - fan-out
```

Cách này giảm mutex contention và đơn giản hóa shutdown/backpressure.

### 4. Memory pooling có kiểm soát

Sau khi profiling chứng minh allocation là nút thắt, dùng `sync.Pool` cho:

- Packet payload buffers.
- Batch slices.
- Serialization buffers.
- PCAPNG staging buffers.

### Tiêu chí hoàn thành

- Không copy payload theo số subscriber.
- Giảm allocation/packet và GC pause có số đo cụ thể.
- Live stream và recorder vẫn độc lập khi một consumer chậm.

## Giai đoạn 2 — Capture engine AF_PACKET

Bổ sung implementation mới phía sau `CaptureEngine`:

```text
NIC → kernel ring buffer → AF_PACKET TPACKET_V3 → agent batch
```

### Cấu hình dự kiến

```text
--capture-engine=afpacket
--capture-ring-size=256MiB
--capture-block-size=1MiB
```

### Yêu cầu

- Giữ `dumpcap` làm fallback.
- Hỗ trợ BPF filter tương đương.
- Xuất metric kernel/ring drop.
- Benchmark với nhiều packet size và nhiều interface.

### Khi nào mới dùng eBPF

Chỉ cân nhắc eBPF khi cần filter/sampling sớm trong kernel, flow aggregation hoặc
packet rate vượt khả năng AF_PACKET. Với yêu cầu lưu full packet cho Wireshark,
AF_PACKET là bước đơn giản hơn.

## Giai đoạn 3 — Packed transport protocol v2

Thay repeated protobuf object cho từng packet bằng một payload liên tục và các mảng
metadata/offset:

```protobuf
message PackedPacketBatch {
  string session_id = 1;
  string node_name = 2;
  string interface_name = 3;
  repeated int64 timestamp_ns = 4;
  repeated uint32 captured_lengths = 5;
  repeated uint32 original_lengths = 6;
  repeated uint32 offsets = 7;
  bytes payload = 8;
}
```

### Yêu cầu

- Có version negotiation giữa agent và server.
- Agent/server cũ vẫn hoạt động trong giai đoạn migration.
- Giới hạn batch theo cả packet count và byte size.
- Benchmark allocation, encoded size và gRPC latency.

## Giai đoạn 4 — Tách control plane và data plane

Tách traffic điều khiển khỏi packet traffic:

```text
Control stream
  - register
  - heartbeat
  - start/stop
  - status

Data stream
  - packet batches
```

Khi cần, mở nhiều data lane theo interface hoặc receive queue. Tiếp tục dùng gRPC
trước; chỉ đánh giá protocol khác khi gRPC tuning không đạt mục tiêu.

## Giai đoạn 5 — Storage pipeline

Biến recorder thành local spool:

```text
packet batch → local NVMe segment → finalize → background upload
```

### Công việc

- Ưu tiên ghi nóng vào local NVMe.
- Buffer ghi khoảng 1–4 MiB.
- Rotation theo dung lượng, `fsync` định kỳ và khi finalize.
- Upload segment hoàn thành lên S3/MinIO ở background nếu cần lưu dài hạn.
- Bổ sung quota tổng storage và retention rõ ràng.
- Không ghi object storage hoặc NFS trực tiếp trên hot path.

### Chính sách khi hết quota

Phải chọn và cấu hình rõ một trong các hành vi:

- Dừng recording nhưng tiếp tục live stream.
- Dừng toàn bộ session.
- Xóa segment cũ theo retention policy.

Không được âm thầm ghi đè hoặc xóa dữ liệu.

## Giai đoạn 6 — Horizontal scaling và HA

Kiến trúc mục tiêu:

```text
                 Control service
                /       |       \
Agent nodes → Ingest-1  Ingest-2  Ingest-3
                  \       |       /
                   Object storage
```

### Nguyên tắc

- Một session thuộc về đúng một ingest server.
- Route bằng consistent hashing theo `session_id`.
- PostgreSQL hoặc Redis chỉ lưu metadata/control state.
- Packet data không đi qua database.
- File segment lưu ở object storage khi cần HA.
- Client được redirect hoặc proxy tới ingest server sở hữu session.

Không triển khai active-active cho cùng một session ở bước đầu.

## Thứ tự ưu tiên đề xuất

1. Benchmark, profiling và metric theo stage.
2. Rebatch sau reorder.
3. Loại bỏ clone payload theo subscriber.
4. Single-owner session event loop.
5. AF_PACKET TPACKET_V3, giữ dumpcap fallback.
6. Packed protocol v2.
7. Local spool và background object upload.
8. Horizontal scaling và external session state.

## Ngoài phạm vi ban đầu

- Chuyển ngay toàn bộ capture sang eBPF.
- Tự xây transport protocol thay gRPC trước khi benchmark.
- Ghi packet trực tiếp vào PostgreSQL/Redis.
- Active-active processing cho một session.
- Tối ưu zero-copy phức tạp khi allocation chưa được chứng minh là nút thắt.

