package healer

import (
	"testing"
)

func TestOffsetFetchRequest(t *testing.T) {
	var (
		partitionID int32  = 0
		clientID    string = "healer"
		topic       string = "test"
		groupID     string = "hangout"
		version     uint16
	)

	r := NewOffsetFetchRequest(0, clientID, groupID)

	if r.Length() != 29 {
		t.Error("offsetcommit request payload length should be 29")
	}

	r.AddPartiton(topic, partitionID)
	if r.Length() != 43 {
		t.Error("offsetcommit request payload length should be 43")
	}

	r.AddPartiton(topic, partitionID)
	if r.Length() != 43 {
		t.Error("offsetcommit request payload length should be 43")
	}

	r.AddPartiton(topic, partitionID+1)
	if r.Length() != 47 {
		t.Error("offsetcommit request payload length should be 47")
	}

	payload := r.Encode(version)
	if len(payload) != 51 {
		t.Error("offsetcommit request payload length should be 51")
	}
}
