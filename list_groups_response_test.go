package healer

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestListGroupsResponseEncodeDecode(t *testing.T) {
	convey.Convey("Test ListGroupsResponse Encode and Decode", t, func() {

		for _, version := range availableVersions[API_ListGroups] {
			t.Logf("version: %v", version)

			header := NewResponseHeader(API_ListGroups, version)
			header.CorrelationID = 1
			header.TaggedFields = mockTaggedFields(header.IsFlexible())

			res := &ListGroupsResponse{
				ResponseHeader: header,
				ErrorCode:      0,
				ThrottleTimeMS: 100,
				Groups: []*Group{
					{
						GroupID:      "group1",
						ProtocolType: "protocol1",
						GroupState:   "state1",
						GroupType:    "type1",
					},
					{
						GroupID:      "group2",
						ProtocolType: "protocol2",
						GroupState:   "state2",
						GroupType:    "type2",
					},
				},
			}

			if version < 1 {
				res.ThrottleTimeMS = 0
			}

			if version < 4 {
				for _, group := range res.Groups {
					group.GroupState = ""
				}
			}

			if version < 5 {
				for _, group := range res.Groups {
					group.GroupType = ""
				}
			}

			payload := res.Encode(version)
			decoded, err := NewListGroupsResponse(payload, version)
			convey.So(err, convey.ShouldBeNil)
			convey.So(decoded, convey.ShouldResemble, res)

			if version >= 5 {
				convey.So(decoded.Groups[0].GroupType, convey.ShouldEqual, "type1")
				convey.So(decoded.Groups[1].GroupType, convey.ShouldEqual, "type2")
			}

			if version >= 4 {
				convey.So(decoded.Groups[0].GroupState, convey.ShouldEqual, "state1")
				convey.So(decoded.Groups[1].GroupState, convey.ShouldEqual, "state2")
			}

			if version >= 1 {
				convey.So(decoded.ThrottleTimeMS, convey.ShouldEqual, 100)
			}
		}
	})
}
