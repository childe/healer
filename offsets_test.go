package healer

import (
	"os"
	"testing"
)

func TestGenOffsetsRequest(t *testing.T) {
	correlationID := int32(os.Getpid())
	partitionID := uint32(0)
	clientID := "healer"
	timeValue := int64(0)
	offsets := uint32(10)
	topic := "test"

	offsetsRequest := NewOffsetsRequest(topic, []uint32{partitionID}, timeValue, offsets, correlationID, clientID)

	payload := offsetsRequest.Encode()
	if len(payload) != 54 {
		t.Error("offsets request payload length should be 54")
	}
}
