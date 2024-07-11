package healer

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestIncrementalAlterConfigs(t *testing.T) {
	Convey("test encode and decode IncrementalAlterConfigs", t, func() {
		clientID := "healer-test"
		req := NewIncrementalAlterConfigsRequest(clientID)
		req.SetValidateOnly(true)

		resourceType := ConvertConfigResourceType("topic")
		resourceName := "topic-name"
		req.AddConfig(resourceType, resourceName, "unclean.leader.election.enable", "false")

		payload := req.Encode(0)

		decodedReq := DecodeIncrementalAlterConfigsRequest(payload, 0)
		So(req, ShouldResemble, decodedReq)
	})
}
