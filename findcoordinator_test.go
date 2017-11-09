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
}
