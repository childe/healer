package healer

import "testing"

func TestOffsetCommitRequest(t *testing.T) {
	var (
		partitionID int32
		clientID    = "healer"
		offset      int64
		topic       = "test"
		groupID     = "hangout"
	)
	broker, err := NewBroker(*brokerAddress, -1, DefaultBrokerConfig())

	r := NewOffsetCommitRequest(0, clientID, groupID)

	if r.Length() != 29 {
		t.Error("offsetcommit request payload length should be 29")
	}

	r.AddPartiton(topic, partitionID, offset, "")
	if r.Length() != 53 {
		t.Error("offsetcommit request payload length should be 53")
	}

	r.AddPartiton(topic, partitionID, offset, "")
	if r.Length() != 53 {
		t.Error("offsetcommit request payload length should be 53")
	}

	r.AddPartiton(topic, partitionID+1, offset+1, "")
	if r.Length() != 67 {
		t.Error("offsetcommit request payload length should be 67")
	}

	payload := r.Encode()
	if len(payload) != 71 {
		t.Error("offsetcommit request payload length should be 71")
	}

	responseBuf, err := broker.Request(r)
	if err != nil {
		t.Errorf("requet offsetcommit error:%s", err)
	}

	_, err = NewOffsetCommitResponse(responseBuf)
	if err != nil {
		t.Errorf("decode offsetcommit response error:%s", err)
	}
	t.Log("get offsetcommit response")

	broker.Close()
}
