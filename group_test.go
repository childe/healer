package healer

import (
	"encoding/json"
	"testing"
)

func TestGroup(t *testing.T) {
	var (
		clientID       = "healer"
		groupID        = "hangout.test"
		groups         = []string{"hangout", "hangout.test"}
		generationID   int32
		memberID             = ""
		sessionTimeout int32 = 30000
		protocolType         = "consumer"
	)

	brokers, err := NewBrokers(*brokersList, clientID, DefaultBrokerConfig())
	if err != nil {
		t.Errorf("new brokers from %s error: %s", *brokersList, err)
	}

	coordinatorResponse, err := brokers.FindCoordinator(clientID, groupID)
	if err != nil {
		t.Error(err)
	}

	coordinator, err := brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
	if err != nil {
		t.Error(err)
	}

	// join group
	joinGroupRequest := NewJoinGroupRequest(clientID, groupID, sessionTimeout, memberID, protocolType)
	joinGroupRequest.AddGroupProtocal(&GroupProtocol{"range", []byte{}})

	responseBytes, err := coordinator.Request(joinGroupRequest)
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
	generationID = joinGroupResponse.GenerationID
	memberID = joinGroupResponse.MemberID
	syncGroupRequest := NewSyncGroupRequest(clientID, groupID, generationID, memberID, nil)

	responseBytes, err = coordinator.Request(syncGroupRequest)
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
	leaveGroupRequest := NewLeaveGroupRequest(clientID, groupID, memberID)

	responseBytes, err = coordinator.Request(leaveGroupRequest)

	leaveGroupResponse, err := NewLeaveGroupResponse(responseBytes)
	if err != nil {
		t.Errorf("try to get leave_group response error:%s", err)
	} else {
		b, _ := json.Marshal(leaveGroupResponse)
		t.Logf("leave response: %s", b)
	}

	// describe group
	describeGroupRequest := NewDescribeGroupsRequest(clientID, groups)

	responseBytes, err = coordinator.Request(describeGroupRequest)

	describeGroupResponse, err := NewDescribeGroupsResponse(responseBytes)
	if err != nil {
		t.Errorf("try to get describe_group response error:%s", err)
	} else {
		b, _ := json.Marshal(describeGroupResponse)
		t.Logf("describe response: %s", b)
	}

	// heartbeat
	heartbeatRequest := NewHeartbeatRequest(clientID, groupID, generationID, memberID)

	responseBytes, err = coordinator.Request(heartbeatRequest)
	if err != nil {
		t.Errorf("failed to send heartbeat request:%s", err)
	}
}
