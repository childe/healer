package healer

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/golang/glog"
)

func TestJoinGroup(t *testing.T) {
	correlationID := int32(os.Getpid())
	correlationID = 9
	clientID := "healer"
	groupID := "hangout.test"
	var sessionTimeout int32 = 30000
	memberID := ""
	protocolType := "consumer"

	request := NewJoinGroupRequest(correlationID, clientID, groupID, sessionTimeout, memberID, protocolType)
	request.AddGroupProtocal("range", []byte{})

	payload := request.Encode()

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
		t.Errorf("try to get join_group response error:%s", err)
	} else {
		b, _ := json.Marshal(response)
		glog.Infof("%s", b)
	}
}
