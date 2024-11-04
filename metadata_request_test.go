package healer

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestMetadataReqeustEncode(t *testing.T) {
	convey.Convey("Test MetadataRequest Encode", t, func() {
		var version uint16 = 7
		requestHeader := &RequestHeader{
			APIKey:     API_MetadataRequest,
			APIVersion: version,
			ClientID:   "healer",
			TaggedFields: TaggedFields{
				{
					Tag:  1,
					Data: []byte("tagged field 1"),
				},
				{
					Tag:  2,
					Data: []byte("tagged field 2"),
				},
			},
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
