package healer

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestDescribeAclsResponseEncodeAndDecode(t *testing.T) {
	convey.Convey("Test DescribeAclsResponse encode and decode", t, func() {
		var errorMessage = "test error"
		var version uint16 = 0
		for i := 0; i < len(availableVersions[API_DescribeAcls]); i++ {
			version = availableVersions[API_DescribeAcls][i]
			t.Logf("test version: %v", version)
			original := DescribeAclsResponse{
				CorrelationID:  123,
				ThrottleTimeMs: 1000,
				ErrorCode:      0,
				ErrorMessage:   &errorMessage,
				Resources: []AclResource{
					{
						ResourceType: 4,
						ResourceName: "test-topic",
						Acls: []Acl{
							{
								Principal:      "User:test",
								Host:           "*",
								Operation:      2,
								PermissionType: 3,
							},
							{
								Principal:      "User:test2",
								Host:           "1.2.3.4",
								Operation:      4,
								PermissionType: 1,
							},
						},
					},
				},
			}

			encoded, err := original.Encode(version)
			if err != nil {
				t.Fatalf("failed to encode DescribeAclsResponse: %v", err)
			}

			decoded, err := NewDescribeAclsResponse(encoded, version)
			if err != nil {
				t.Fatalf("failed to decode DescribeAclsResponse: %v", err)
			}

			convey.So(decoded, convey.ShouldResemble, original)
		}
	})
}
