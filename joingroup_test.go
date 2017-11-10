package healer

import (
	"os"
	"testing"
)

func TestJoinGroup(t *testing.T) {
	correlationID := int32(os.Getpid())
	clientID := "healer"
	groupID := "healer.topicname"
	var sessionTimeout int32 = 6000
	memberID := ""
	protocolType := ""

	request := NewJoinGroupRequest(correlationID, clientID, groupID, sessionTimeout, memberID, protocolType)

	payload := request.Encode()
	if len(payload) != 50 {
		t.Error("offsets request payload length should be 50")
	}

	broker, err := NewBroker(*brokerAddress, "healer", -1, 60, 60)
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

	response, err := NewJoinGroupResponse(responseBytes)
	if err != nil {
		t.Errorf("decode join_group response error:%s", err)
	} else {
		t.Logf("join_group response errorcode:%d", response.ErrorCode)
		//t.Logf("join_group response Coordinator:%s:%d (%d)", response.Coordinator.host, response.Coordinator.port, response.Coordinator.nodeID)
	}
}
