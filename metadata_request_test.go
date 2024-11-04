package healer

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/smartystreets/goconvey/convey"
)

func TestMetadataReqeustEncode(t *testing.T) {
	mockey.PatchConvey("Test MetadataRequest Encode", t, func() {
		var version uint16 = 7
		var clientID string = "healer"
		requestHeader := &RequestHeader{
			APIKey:     API_MetadataRequest,
			APIVersion: version,
			ClientID:   &clientID,
		}
		original := MetadataRequest{
			RequestHeader:          requestHeader,
			Topics:                 []string{"test-topic"},
			AllowAutoTopicCreation: true,
		}

		payload := original.Encode(version)

		decoded, err := DecodeMetadataRequest(payload, version)
		if err != nil {
			t.Fatalf("decode metadata error: %v", err)
		}
		convey.So(decoded, convey.ShouldResemble, original)
	})
}
