package healer

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestAclCreationLengthEncodeDecode(t *testing.T) {
	convey.Convey("Test AclCreation encode and decode", t, func() {
		var version uint16 = 0
		var clientID string = "healer"
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
			},
			TaggedFields: nil,
		}

		encoded, err := original.Encode()
		if err != nil {
			t.Fatalf("encode error: %v", err)
		}

		decoded, err := DecodeCreateAclsRequest(encoded, version)
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}

		convey.So(original, convey.ShouldResemble, decoded)

	})
}
