package healer

import "testing"

func TestFindCoordinator(t *testing.T) {
	var (
		clientID string = "healer"
		groupID  string = "healer.topicname"
		version  uint16 = 0
	)

	request := NewFindCoordinatorRequest(clientID, groupID)

	payload := request.Encode(version)
	if len(payload) != 38 {
		t.Error("offsets request payload length should be 38")
	}
}
