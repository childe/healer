package healer

import (
	"os"
	"testing"
)

func TestGenOffsetsRequest(t *testing.T) {
	correlationID := int32(os.Getpid())
	partitionID := 0
	clientID := "healer"
	timeValue := int64(0)
	offsets := uint32(10)
	topic := "test"

	requestHeader := &RequestHeader{
		ApiKey:        API_OffsetRequest,
		ApiVersion:    0,
		CorrelationId: correlationID,
		ClientId:      clientID,
	}

	partitionOffsetRequestInfos := make(map[uint32]*PartitionOffsetRequestInfo)
	partitionOffsetRequestInfos[uint32(partitionID)] = &PartitionOffsetRequestInfo{
		Time:               timeValue,
		MaxNumberOfOffsets: offsets,
	}
	topicOffsetRequestInfos := make(map[string]map[uint32]*PartitionOffsetRequestInfo)
	topicOffsetRequestInfos[topic] = partitionOffsetRequestInfos

	offsetsReqeust := &OffsetsRequest{
		RequestHeader: requestHeader,
		ReplicaId:     -1,
		RequestInfo:   topicOffsetRequestInfos,
	}

	payload := offsetsReqeust.Encode()
	if len(payload) != 54 {
		t.Error("offsets request payload length should be 54")
	}
}
