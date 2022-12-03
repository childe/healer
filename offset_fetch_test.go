package healer

import (
	"testing"

	"github.com/golang/glog"
)

func TestOffsetFetchRequest(t *testing.T) {
	var (
		partitionID int32  = 0
		clientID    string = "healer"
		topic       string = "test"
		groupID     string = "hangout"
		version     uint16
	)
	broker, err := NewBroker(*brokerAddress, -1, DefaultBrokerConfig())
	if err != nil {
		t.Errorf("create broker error:%s", err)
	}

	r := NewOffsetFetchRequest(0, clientID, groupID)

	glog.Info(r.Length())
	if r.Length() != 29 {
		t.Error("offsetcommit request payload length should be 29")
	}

	r.AddPartiton(topic, partitionID)
	if r.Length() != 43 {
		t.Error("offsetcommit request payload length should be 43")
	}

	r.AddPartiton(topic, partitionID)
	glog.Info(r.Length())
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

	responseBuf, err := broker.Request(r)
	if err != nil {
		t.Errorf("requet offsetcommit error:%s", err)
	}

	_, err = NewOffsetFetchResponse(responseBuf)
	if err != nil {
		t.Errorf("parse offsetfetch response error:%s", err)
	}
	t.Log("get offsetfetch response")

	broker.Close()
}
