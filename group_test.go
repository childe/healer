package healer

import (
	"encoding/json"
	"testing"

	"github.com/golang/glog"
)

func TestGroup(t *testing.T) {
	var (
		correlationID     int32  = 11
		clientID          string = "healer"
		groupID           string = "hangout.test"
		groupGenerationID int32
		memberID          string = ""
		sessionTimeout    int32  = 30000
		protocolType      string = "consumer"
	)

	// join group
	joinGroupRequest := NewJoinGroupRequest(correlationID, clientID, groupID, sessionTimeout, memberID, protocolType)
	joinGroupRequest.AddGroupProtocal("range", []byte{})

	payload := joinGroupRequest.Encode()

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

	joinGroupResponse, err := NewJoinGroupResponse(responseBytes)
	if err != nil {
		t.Errorf("try to get join_group response error:%s", err)
	} else {
		b, _ := json.Marshal(joinGroupResponse)
		glog.Infof("%s", b)
	}

	// sync group
	correlationID = 14
	groupGenerationID = joinGroupResponse.GenerationID
	memberID = joinGroupResponse.MemberID
	syncGroupRequest := NewSyncGroupRequest(correlationID, clientID, groupID, groupGenerationID, memberID)
	payload = syncGroupRequest.Encode()

	responseBytes, err = broker.request(payload)
	if err != nil {
		t.Errorf("send sync_group request error:%s", err)
	} else {
		t.Logf("got response from sync_group request:%d bytes", len(responseBytes))
	}

	syncGroupResponse, err := NewSyncGroupResponse(responseBytes)
	if err != nil {
		t.Errorf("try to get sync_group response error:%s", err)
	} else {
		b, _ := json.Marshal(syncGroupResponse)
		glog.Infof("%s", b)
	}
}
