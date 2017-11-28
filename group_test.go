package healer

import (
	"encoding/json"
	"testing"
)

func TestGroup(t *testing.T) {
	var (
		correlationID  uint32
		clientID       string   = "healer"
		groupID        string   = "hangout.test"
		groups         []string = []string{"hangout", "hangout.test"}
		generationID   int32
		memberID       string = ""
		sessionTimeout int32  = 30000
		protocolType   string = "consumer"
	)

	// join group
	correlationID = uint32(API_JoinGroup)
	joinGroupRequest := NewJoinGroupRequest(correlationID, clientID, groupID, sessionTimeout, memberID, protocolType)
	joinGroupRequest.AddGroupProtocal(&GroupProtocol{"range", []byte{}})

	payload := joinGroupRequest.Encode()

	broker, err := NewBroker(*brokerAddress, -1, 60, 60)
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
		t.Logf("join group response: %s", b)
	}

	// sync group
	correlationID = uint32(API_SyncGroup)
	generationID = joinGroupResponse.GenerationID
	memberID = joinGroupResponse.MemberID
	syncGroupRequest := NewSyncGroupRequest(correlationID, clientID, groupID, generationID, memberID, nil)
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
		t.Logf("sync response: %s", b)
	}

	// leave group
	correlationID = uint32(API_LeaveGroup)
	leaveGroupRequest := NewLeaveGroupRequest(correlationID, clientID, groupID, memberID)
	payload = leaveGroupRequest.Encode()

	responseBytes, err = broker.request(payload)

	leaveGroupResponse, err := NewLeaveGroupResponse(responseBytes)
	if err != nil {
		t.Errorf("try to get leave_group response error:%s", err)
	} else {
		b, _ := json.Marshal(leaveGroupResponse)
		t.Logf("leave response: %s", b)
	}

	// describe group
	correlationID = uint32(API_DescribeGroups)
	describeGroupRequest := NewDescribeGroupsRequest(correlationID, clientID, groups)
	payload = describeGroupRequest.Encode()

	responseBytes, err = broker.request(payload)

	describeGroupResponse, err := NewDescribeGroupsResponse(responseBytes)
	if err != nil {
		t.Errorf("try to get describe_group response error:%s", err)
	} else {
		b, _ := json.Marshal(describeGroupResponse)
		t.Logf("describe response: %s", b)
	}

	// heartbeat
	correlationID = uint32(API_Heartbeat)
	heartbeatRequest := NewHeartbeatRequest(correlationID, clientID, groupID, generationID, memberID)

	responseBytes, err = broker.request(heartbeatRequest.Encode())
	if err != nil {
		t.Errorf("failed to send heartbeat request:%s", err)
	}
}
