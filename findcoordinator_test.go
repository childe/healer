package healer

import "testing"

func TestFindCoordinator(t *testing.T) {
	var (
		clientID string = "healer"
		groupID  string = "healer.topicname"
	)

	request := NewFindCoordinatorRequest(clientID, groupID)

	payload := request.Encode()
	if len(payload) != 38 {
		t.Error("offsets request payload length should be 38")
	}

	broker, err := NewBroker(*brokerAddress, -1, DefaultBrokerConfig())
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	} else {
		t.Logf("got new broker from %s %s %d", *brokerAddress, "healer", -1)
	}

	responseBytes, err := broker.request(payload)
	if err != nil {
		t.Errorf("send findcoordinator request error:%s", err)
	} else {
		t.Logf("got response from findcoordinator request:%d bytes", len(responseBytes))
	}

	response, err := NewFindCoordinatorResponse(responseBytes)
	if err != nil {
		t.Errorf("decode findcoordinator response error:%s", err)
	} else {
		t.Logf("findcoordinator response errorcode:%d", response.ErrorCode)
		t.Logf("findcoordinator response Coordinator:%s:%d (%d)", response.Coordinator.Host, response.Coordinator.Port, response.Coordinator.NodeID)
	}
}
