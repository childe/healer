package healer

import (
	"os"
	"testing"
)

func TestGenFindCoordinatorRequest(t *testing.T) {
	correlationID := int32(os.Getpid())
	clientID := "healer"
	groupID := "healer.topicname"

	request := NewFindCoordinatorRequest(correlationID, clientID, groupID)

	payload := request.Encode()
	if len(payload) != 38 {
		t.Error("offsets request payload length should be 54")
	}

	broker, err := NewBroker(*brokerAddress, "healer", -1, 60, 30)
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	} else {
		t.Logf("got new broker from %s %s %d", *brokerAddress, "healer", -1)
	}

	response, err := broker.request(payload)
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	} else {
		t.Logf("got response from FindCoordinator request:%s", response)
	}
}
