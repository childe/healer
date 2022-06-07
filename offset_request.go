package healer

/*
This API describes the valid offset range available for a set of topic-partitions. As with the produce and fetch APIs requests must be directed to the broker that is currently the leader for the partitions in question. This can be determined using the metadata API.

The response contains the starting offset of each segment for the requested partition as well as the "log end offset" i.e. the offset of the next message that would be appended to the given partition.

We agree that this API is slightly funky.

Offset Request

	OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
	  ReplicaId => int32
	  TopicName => string
	  Partition => int32
	  Time => int64
	  MaxNumberOfOffsets => int32

Field	Decription
Time	Used to ask for all messages before a certain time (ms). There are two special values. Specify -1 to receive the latest offset (i.e. the offset of the next coming message) and -2 to receive the earliest available offset. Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
*/

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
		APIKey:     API_OffsetRequest,
		APIVersion: 0,
		ClientID:   clientID,
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
	requestLength := 8 + 2 + len(offsetR.RequestHeader.ClientID) + 4
	requestLength += 4
	for topicName, partitionInfo := range offsetR.RequestInfo {
		requestLength += 2 + len(topicName) + 4 + len(partitionInfo)*16
	}
	payload := make([]byte, 4+requestLength)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset = offsetR.RequestHeader.Encode(payload, offset)

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
			binary.BigEndian.PutUint32(payload[offset:], partitionOffsetRequestInfo.MaxNumberOfOffsets)
			offset += 4
		}
	}

	return payload
}
