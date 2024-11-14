package healer

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestListGroupsRequestEncodeDecode(t *testing.T) {
	convey.Convey("Test ListGroupsRequest Encode and Decode", t, func() {
		clientID := "testClient"
		request := NewListGroupsRequest(clientID)

		for _, version := range availableVersions[API_ListGroups] {
			t.Logf("version: %v", version)

			if version < 3 {
				request.IncludeAuthorizedOperations = false
			}

			encodedRequest := request.Encode(version)
			decodedRequest, err := DecodeListGroupsRequest(encodedRequest)

			convey.So(err, convey.ShouldBeNil)
			convey.So(request, convey.ShouldResemble, decodedRequest)
		}
	})
}
