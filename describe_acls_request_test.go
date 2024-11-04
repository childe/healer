package healer

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestDescribeAclsRequestEncodeAndDecode(t *testing.T) {
	convey.Convey("Test DescribeAclsRequest Encode and Decode at version 1 and 2", t, func() {
		var (
			resourceName = "test-resource"
			principal    = "test-principal"
			host         = "test-host"
		)
		var version uint16
		for version = 0; version <= 2; version++ {
			original := DescribeAclsRequest{
				RequestHeader: RequestHeader{
					APIKey:        API_DescribeAcls,
					APIVersion:    version,
					CorrelationID: 10,
				},
				DescribeAclsRequestBody: DescribeAclsRequestBody{
					ResourceType:   1,
					ResourceName:   &resourceName,
					PatternType:    3,
					Principal:      &principal,
					Host:           &host,
					Operation:      2,
					PermissionType: 3,
				},
			}

			if version == 0 {
				original.PatternType = 0
			}

			encoded := original.Encode(version)

			decoded, err := DecodeDescribeAclsRequest(encoded, version)
			if err != nil {
				t.Errorf("decode error: %v", err)
			}

			convey.So(decoded, convey.ShouldResemble, original)
		}
	})
}
