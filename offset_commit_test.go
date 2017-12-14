package healer

import "testing"

func TestGenOffsetCommitRequest(t *testing.T) {
	var (
		correlationID uint32 = uint32(API_OffsetCommitRequest)
		partitionID   int32  = 0
		clientID      string = "healer"
		offset        uint64 = 10
		topic         string = "test"
	)

	r := NewOffsetCommitRequest(correlationID, clientID)

	if r.Length() != 22 {
		t.Error("offsetcommit request payload length should be 22")
	}

	r.AddPartiton(topic, partitionID, offset, "")
	if r.Length() != 46 {
		t.Error("offsetcommit request payload length should be 46")
	}

	r.AddPartiton(topic, partitionID, offset, "")
	if r.Length() != 46 {
		t.Error("offsetcommit request payload length should be 46")
	}

	r.AddPartiton(topic, partitionID+1, offset+1, "")
	if r.Length() != 60 {
		t.Error("offsetcommit request payload length should be 60")
	}

	payload := r.Encode()
	if len(payload) != 64 {
		t.Error("offsetcommit request payload length should be 64")
	}
}
