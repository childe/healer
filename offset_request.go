package healer

import (
	"encoding/binary"
)

type PartitionOffsetRequestInfo struct {
	Time               int64
	MaxNumberOfOffsets uint32
}

type OffsetsRequest struct {
	*RequestHeader
	ReplicaId   int32
	RequestInfo map[string]map[int32]*PartitionOffsetRequestInfo
}

// request only ONE topic
func NewOffsetsRequest(topic string, partitionIDs []int32, timeValue int64, offsets uint32, clientID string) *OffsetsRequest {
	requestHeader := &RequestHeader{
		APIKey:   API_OffsetRequest,
		ClientID: &clientID,
	}

	partitionOffsetRequestInfos := make(map[int32]*PartitionOffsetRequestInfo)
	for _, id := range partitionIDs {
		partitionOffsetRequestInfos[id] = &PartitionOffsetRequestInfo{
			Time:               timeValue,
			MaxNumberOfOffsets: offsets,
		}
	}
	topicOffsetRequestInfos := make(map[string]map[int32]*PartitionOffsetRequestInfo)
	topicOffsetRequestInfos[topic] = partitionOffsetRequestInfos

	offsetsRequest := &OffsetsRequest{
		RequestHeader: requestHeader,
		ReplicaId:     -1,
		RequestInfo:   topicOffsetRequestInfos,
	}

	return offsetsRequest
}

func (offsetR *OffsetsRequest) Encode(version uint16) []byte {
	requestLength := 8 + 2 + len(*offsetR.RequestHeader.ClientID) + 4
	requestLength += 4
	for topicName, partitionInfo := range offsetR.RequestInfo {
		requestLength += 2 + len(topicName) + 4 + len(partitionInfo)*16
	}

	payload := make([]byte, 4+requestLength)
	offset := 0

	defer func() {
		binary.BigEndian.PutUint32(payload, uint32(offset-4))
	}()

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset += offsetR.RequestHeader.Encode(payload[offset:])

	binary.BigEndian.PutUint32(payload[offset:], uint32(offsetR.ReplicaId))
	offset += 4

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(offsetR.RequestInfo)))
	offset += 4
	for topicName, partitionOffsetRequestInfos := range offsetR.RequestInfo {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(topicName)))
		offset += 2

		offset += copy(payload[offset:], topicName)

		binary.BigEndian.PutUint32(payload[offset:], uint32(len(partitionOffsetRequestInfos)))
		offset += 4
		for partitionID, partitionOffsetRequestInfo := range partitionOffsetRequestInfos {
			binary.BigEndian.PutUint32(payload[offset:], uint32(partitionID))
			offset += 4

			binary.BigEndian.PutUint64(payload[offset:], uint64(partitionOffsetRequestInfo.Time))
			offset += 8
			if version == 0 {
				binary.BigEndian.PutUint32(payload[offset:], partitionOffsetRequestInfo.MaxNumberOfOffsets)
				offset += 4

			}
		}
	}

	return payload[:offset]
}
