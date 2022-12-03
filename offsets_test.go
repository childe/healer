package healer

import "testing"

func TestGenOffsetsRequest(t *testing.T) {
	var (
		partitionID int32  = 0
		clientID    string = "healer"
		timeValue   int64  = 0
		offsets     uint32 = 10
		topic       string = "test"
		version     uint16
	)

	offsetsRequest := NewOffsetsRequest(topic, []int32{partitionID}, timeValue, offsets, clientID)

	payload := offsetsRequest.Encode(version)
	if len(payload) != 54 {
		t.Error("offsets request payload length should be 54")
	}
}
