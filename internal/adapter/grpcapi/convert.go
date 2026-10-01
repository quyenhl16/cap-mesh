package grpcapi

import (
	"time"

	capmeshv1 "github.com/quyenhl16/cap-mesh/api/capmesh/v1"
	"github.com/quyenhl16/cap-mesh/internal/core/domain"
)

func PacketBatchToProto(batch domain.PacketBatch) *capmeshv1.PacketBatch {
	out := &capmeshv1.PacketBatch{SessionId: batch.SessionID, NodeName: batch.NodeName, InterfaceName: batch.InterfaceName, Source: CaptureSourceToProto(batch.Source), Packets: make([]*capmeshv1.Packet, 0, len(batch.Packets))}
	for _, packet := range batch.Packets {
		out.Packets = append(out.Packets, &capmeshv1.Packet{TimestampNs: packet.Timestamp.UnixNano(), CapturedLength: packet.CapturedLength, OriginalLength: packet.OriginalLength, LinkType: packet.LinkType, SequenceNumber: packet.SequenceNumber, Data: packet.Data})
	}
	return out
}

func PacketBatchFromProto(batch *capmeshv1.PacketBatch) domain.PacketBatch {
	out := domain.PacketBatch{SessionID: batch.GetSessionId(), NodeName: batch.GetNodeName(), InterfaceName: batch.GetInterfaceName(), Source: CaptureSourceFromProto(batch.GetSource()), Packets: make([]domain.Packet, 0, len(batch.GetPackets()))}
	for _, packet := range batch.GetPackets() {
		out.Packets = append(out.Packets, domain.Packet{Timestamp: time.Unix(0, packet.GetTimestampNs()), CapturedLength: packet.GetCapturedLength(), OriginalLength: packet.GetOriginalLength(), LinkType: packet.GetLinkType(), SequenceNumber: packet.GetSequenceNumber(), Data: append([]byte(nil), packet.GetData()...)})
	}
	return out
}

func SessionToProto(session domain.Session) *capmeshv1.CaptureSession {
	var expiresAt int64
	if !session.ExpiresAt.IsZero() {
		expiresAt = session.ExpiresAt.UnixNano()
	}
	return &capmeshv1.CaptureSession{Id: session.ID, Nodes: session.Nodes, LogicalInterface: session.LogicalInterface, Filter: session.Filter, Snaplen: session.Snaplen, CreatedAtNs: session.CreatedAt.UnixNano(), ExpiresAtNs: expiresAt, Status: string(session.Status), Message: session.Message, Targets: CaptureTargetsToProto(session.Targets), Mode: string(session.Mode)}
}

func CaptureSourceToProto(source domain.CaptureSource) *capmeshv1.CaptureSource {
	if source.ID == "" {
		return nil
	}
	return &capmeshv1.CaptureSource{Id: source.ID, TargetId: source.TargetID, TargetType: source.TargetType, NodeName: source.NodeName, LogicalInterface: source.LogicalInterface, InterfaceName: source.InterfaceName, Namespace: source.Namespace, PodName: source.PodName, PodUid: source.PodUID, PodIp: source.PodIP}
}

func CaptureSourceFromProto(source *capmeshv1.CaptureSource) domain.CaptureSource {
	if source == nil {
		return domain.CaptureSource{}
	}
	return domain.CaptureSource{ID: source.GetId(), TargetID: source.GetTargetId(), TargetType: source.GetTargetType(), NodeName: source.GetNodeName(), LogicalInterface: source.GetLogicalInterface(), InterfaceName: source.GetInterfaceName(), Namespace: source.GetNamespace(), PodName: source.GetPodName(), PodUID: source.GetPodUid(), PodIP: source.GetPodIp()}
}

func CaptureTargetsToProto(targets []domain.CaptureTarget) []*capmeshv1.CaptureTarget {
	out := make([]*capmeshv1.CaptureTarget, 0, len(targets))
	for _, target := range targets {
		value := &capmeshv1.CaptureTarget{Id: target.ID}
		if target.Interface != nil {
			value.InterfaceTarget = &capmeshv1.InterfaceTarget{Nodes: target.Interface.Nodes, LogicalInterface: target.Interface.LogicalInterface}
		}
		if target.Workload != nil {
			value.WorkloadTarget = &capmeshv1.WorkloadTarget{Namespace: target.Workload.Namespace, Kind: target.Workload.Kind, Name: target.Workload.Name, Direction: target.Workload.Direction, Follow: target.Workload.Follow, MaxPods: target.Workload.MaxPods}
		}
		out = append(out, value)
	}
	return out
}

func CaptureTargetsFromProto(targets []*capmeshv1.CaptureTarget) []domain.CaptureTarget {
	out := make([]domain.CaptureTarget, 0, len(targets))
	for _, target := range targets {
		value := domain.CaptureTarget{ID: target.GetId()}
		if interfaceTarget := target.GetInterfaceTarget(); interfaceTarget != nil {
			value.Interface = &domain.InterfaceTarget{Nodes: append([]string(nil), interfaceTarget.GetNodes()...), LogicalInterface: interfaceTarget.GetLogicalInterface()}
		}
		if workloadTarget := target.GetWorkloadTarget(); workloadTarget != nil {
			value.Workload = &domain.WorkloadTarget{Namespace: workloadTarget.GetNamespace(), Kind: workloadTarget.GetKind(), Name: workloadTarget.GetName(), Direction: workloadTarget.GetDirection(), Follow: workloadTarget.GetFollow(), MaxPods: workloadTarget.GetMaxPods()}
		}
		out = append(out, value)
	}
	return out
}
