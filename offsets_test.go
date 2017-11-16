package healer

import "testing"

func TestGenOffsetsRequest(t *testing.T) {
	var (
		correlationID uint32 = 2
		partitionID   uint32 = 0
		clientID      string = "healer"
		timeValue     int64  = 0
		offsets       uint32 = 10
		topic         string = "test"
	)

	offsetsRequest := NewOffsetsRequest(topic, []uint32{partitionID}, timeValue, offsets, correlationID, clientID)

	payload := offsetsRequest.Encode()
	if len(payload) != 54 {
		t.Error("offsets request payload length should be 54")
	}
}
