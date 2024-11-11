package healer

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestDeleteAclsResponseEncodeDecode(t *testing.T) {
	convey.Convey("Test DeleteAclsResponse Encode and Decode", t, func() {
		for _, version := range availableVersions[API_DeleteAcls] {
			t.Logf("version: %v", version)

			var resourceName = "test-topic"
			var principal = "User:test"
			var host = "*"
			var errorMsg = "mock error"
			header := NewResponseHeader(API_DeleteAcls, version)

			matchingAcl := DeleteAclsMatchingAcl{
				ErrorCode:      0,
				ErrorMessage:   nil,
				ResourceType:   2, // Topic
				ResourceName:   resourceName,
				PatternType:    3, // Match
				Principal:      principal,
				Host:           host,
				Operation:      2, // Write
				PermissionType: 3, // Allow
				TaggedFields:   mockTaggedFields(header.IsFlexible()),
			}

			if version == 0 {
				matchingAcl.PatternType = 0
			}

			filterResult := DeleteAclsFilterResult{
				ErrorCode:    1,
				ErrorMessage: &errorMsg,
				MatchingAcls: []DeleteAclsMatchingAcl{matchingAcl},
				TaggedFields: mockTaggedFields(header.IsFlexible()),
			}

			response := &DeleteAclsResponse{
				ResponseHeader: header,
				ThrottleTimeMs: 100,
				FilterResults:  []DeleteAclsFilterResult{filterResult, filterResult},
				TaggedFields:   mockTaggedFields(header.IsFlexible()),
			}
			response.ResponseHeader.CorrelationID = 100

			encoded := response.Encode()

			decoded, err := DecodeDeleteAclsResponse(encoded, version)
			convey.So(err, convey.ShouldBeNil)

			convey.So(decoded, convey.ShouldResemble, response)
		}
	})
}
