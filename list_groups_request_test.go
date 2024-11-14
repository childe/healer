package healer

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestListGroupsRequestEncodeDecode(t *testing.T) {
	convey.Convey("Test ListGroupsRequest Encode and Decode", t, func() {
		clientID := "testClient"

		for _, version := range availableVersions[API_ListGroups] {
			t.Logf("version: %v", version)

			request := NewListGroupsRequest(clientID)
			request.SetStatesFilter([]string{"testState1", "testState2"})
			request.SetTypesFilter([]string{"testType1", "testType2"})
			request.APIVersion = version
			request.TaggedFields = mockTaggedFields(request.IsFlexible())

			tags := request.tags()

			if version < tags["StatesFilter"] {
				request.StatesFilter = nil
			}
			if version < tags["TypesFilter"] {
				request.TypesFilter = nil
			}

			encodedRequest := request.Encode(version)

			decodedRequest, err := DecodeListGroupsRequest(encodedRequest)

			convey.So(err, convey.ShouldBeNil)
			convey.So(decodedRequest, convey.ShouldResemble, request)
		}
	})
}
