package healer

import "testing"

func TestSyncGroup(t *testing.T) {
	var (
		correlationID     int32  = 14
		clientID          string = "healer"
		groupID           string = "hangout.test"
		groupGenerationID int32  = 30
		memberID          string = ""
	)

	request := NewSyncGroupRequest(correlationID, clientID, groupID, groupGenerationID, memberID)

	payload := request.Encode()

	broker, err := NewBroker(*brokerAddress, "healer", -1, 60, 60)
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	} else {
		t.Logf("got new broker from %s %s %d", *brokerAddress, "healer", -1)
	}

	responseBytes, err := broker.request(payload)
	if err != nil {
		t.Errorf("send sync_group request error:%s", err)
	} else {
		t.Logf("got response from sync_group request:%d bytes", len(responseBytes))
	}

	//response, err := New(responseBytes)
	//if err != nil {
	//t.Errorf("try to get sync_group response error:%s", err)
	//} else {
	//t.Logf("sync_group response errorcode:%d", response.ErrorCode)
	//}

	//b, _ := json.Marshal(response)
	//glog.Infof("%s", b)
}
