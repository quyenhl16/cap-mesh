package grpcapi

import (
	"time"

	capmeshv1 "github.com/quyenhl16/capmesh/api/capmesh/v1"
	"github.com/quyenhl16/capmesh/internal/core/domain"
)

func PacketBatchToProto(batch domain.PacketBatch) *capmeshv1.PacketBatch {
	out := &capmeshv1.PacketBatch{SessionId: batch.SessionID, NodeName: batch.NodeName, InterfaceName: batch.InterfaceName, Packets: make([]*capmeshv1.Packet, 0, len(batch.Packets))}
	for _, packet := range batch.Packets {
		out.Packets = append(out.Packets, &capmeshv1.Packet{TimestampNs: packet.Timestamp.UnixNano(), CapturedLength: packet.CapturedLength, OriginalLength: packet.OriginalLength, LinkType: packet.LinkType, SequenceNumber: packet.SequenceNumber, Data: packet.Data})
	}
	return out
}

func PacketBatchFromProto(batch *capmeshv1.PacketBatch) domain.PacketBatch {
	out := domain.PacketBatch{SessionID: batch.GetSessionId(), NodeName: batch.GetNodeName(), InterfaceName: batch.GetInterfaceName(), Packets: make([]domain.Packet, 0, len(batch.GetPackets()))}
	for _, packet := range batch.GetPackets() {
		out.Packets = append(out.Packets, domain.Packet{Timestamp: time.Unix(0, packet.GetTimestampNs()), CapturedLength: packet.GetCapturedLength(), OriginalLength: packet.GetOriginalLength(), LinkType: packet.GetLinkType(), SequenceNumber: packet.GetSequenceNumber(), Data: append([]byte(nil), packet.GetData()...)})
	}
	return out
}

func SessionToProto(session domain.Session) *capmeshv1.CaptureSession {
	return &capmeshv1.CaptureSession{Id: session.ID, Nodes: session.Nodes, LogicalInterface: session.LogicalInterface, Filter: session.Filter, Snaplen: session.Snaplen, CreatedAtNs: session.CreatedAt.UnixNano(), ExpiresAtNs: session.ExpiresAt.UnixNano(), Status: string(session.Status), Message: session.Message}
}
