package healer

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestAclCreationLengthEncodeDecode(t *testing.T) {
	convey.Convey("Test AclCreation encode and decode", t, func() {
		var version uint16 = 0
		var clientID string = "healer"

		for _, version = range availableVersions[API_CreateAcls] {
			original := CreateAclsRequest{
				RequestHeader: RequestHeader{
					APIKey:        API_CreateAcls,
					APIVersion:    version,
					CorrelationID: 1000,
					ClientID:      &clientID,
					TaggedFields:  nil,
				},
				Creations: []AclCreation{
					{
						ResourceType:        1,
						ResourceName:        "testResource",
						ResourcePatternType: 2,
						Principal:           "testPrincipal",
						Host:                "testHost",
						Operation:           3,
						PermissionType:      4,
					},
					{
						ResourceType:        2,
						ResourceName:        "testResource_2",
						ResourcePatternType: 3,
						Principal:           "testPrincipal_2",
						Host:                "testHost_2",
						Operation:           4,
						PermissionType:      5,
					},
				},
				TaggedFields: nil,
			}
			if version < 1 {
				for i := range original.Creations {
					original.Creations[i].ResourcePatternType = 0
				}
			}

			t.Logf("reqeust version: %+v", version)

			encoded, err := original.Encode()
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			decoded, err := DecodeCreateAclsRequest(encoded)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}

			convey.So(original, convey.ShouldResemble, decoded)
		}
	})
}
