package healer

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestReqeustHeaderEncode(t *testing.T) {
	convey.Convey("Test RequestHeader Encode", t, func() {
		var version uint16 = 7
		original := RequestHeader{
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

		payload := make([]byte, original.length())
		n := original.Encode(payload)
		payload = payload[:n]

		decoded, nn := DecodeRequestHeader(payload, version)
		convey.So(nn, convey.ShouldEqual, n)
		convey.So(decoded, convey.ShouldResemble, original)
	})
}
