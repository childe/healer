package healer

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestDeleteAclsRequestEncodeDecode(t *testing.T) {
	convey.Convey("Test DeleteAclsRequest Encode and Decode", t, func() {
		for _, version := range availableVersions[API_DeleteAcls] {
			t.Logf("version: %v", version)
			var clientID = "healer"
			var resourceName = "test-topic"
			var principal = "User:test"
			var host = "*"

			filter := &DeleteAclsFilter{
				ResourceType:   2, // Topic
				ResourceName:   &resourceName,
				PatternType:    3, // Match
				Principal:      &principal,
				Host:           &host,
				Operation:      2, // Write
				PermissionType: 3, // Allow
			}

			if version == 0 {
				filter.PatternType = 0
			}

			request := NewDeleteAclsRequest(clientID, []*DeleteAclsFilter{filter})
			request.SetCorrelationID(100)
			request.SetVersion(version)

			encoded := request.Encode(version)

			decoded, err := DecodeDeleteAclsRequest(encoded)
			convey.So(err, convey.ShouldBeNil)

			convey.So(decoded, convey.ShouldResemble, request)
		}
	})
}
