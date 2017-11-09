package healer

import (
	"os"
	"testing"
)

func TestJoinGroup(t *testing.T) {
	correlationID := int32(os.Getpid())
	clientID := "healer"
	groupID := "healer.topicname"
	var sessionTimeout int32 = 10
	memberID := ""
	protocolType := ""

	request := NewJoinGroupRequest(correlationID, clientID, groupID, sessionTimeout, memberID, protocolType)

	payload := request.Encode()
	if len(payload) != 50 {
		t.Error("offsets request payload length should be 50")
	}

	broker, err := NewBroker(*brokerAddress, "healer", -1, 60, 30)
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	} else {
		t.Logf("got new broker from %s %s %d", *brokerAddress, "healer", -1)
	}

	responseBytes, err := broker.request(payload)
	if err != nil {
		t.Errorf("send join_group request error:%s", err)
	} else {
		t.Logf("got response from join_group request:%d bytes", len(responseBytes))
	}
}
