package healer

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestCreateAclsResponseEncodeDeocde(t *testing.T) {
	convey.Convey("Test CreateAclsResponse Encode", t, func() {
		errorMsg := "mock error"
		for _, version := range availableVersions[API_CreateAcls] {
			t.Logf("version: %v", version)

			header := NewResponseHeader(API_CreateAcls, version)
			header.CorrelationID = 1

			response := &CreateAclsResponse{
				ResponseHeader: header,
				Results: []AclCreationResult{
					{
						ErrorCode:    1,
						ErrorMessage: &errorMsg,
					},
					{
						ErrorCode:    2,
						ErrorMessage: new(string),
					},
				},
			}

			payload := response.Encode()

			decodedResponse, err := DecodeCreateAclsResponse(payload, version)
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			convey.So(response, convey.ShouldResemble, decodedResponse)
		}
	})
}
