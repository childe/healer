package healer

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/smartystreets/goconvey/convey"
)

func TestReqeustHeaderEncodeV0(t *testing.T) {
	mockey.PatchConvey("Test RequestHeader v0 Encode", t, func() {
		var version uint16 = 7
		original := RequestHeader{
			APIKey:     API_MetadataRequest,
			APIVersion: version,
		}

		mockey.Mock((*RequestHeader).headerVersion).Return(0).Build()

		payload := make([]byte, original.length())
		n := original.Encode(payload)
		payload = payload[:n]

		decoded, nn := DecodeRequestHeader(payload)
		convey.So(nn, convey.ShouldEqual, n)
		convey.So(decoded, convey.ShouldResemble, original)
	})
}
func TestReqeustHeaderEncodeV1(t *testing.T) {
	mockey.PatchConvey("Test RequestHeader v1 Encode", t, func() {
		var version uint16 = 7
		var clientID string = "healer"
		original := RequestHeader{
			APIKey:     API_MetadataRequest,
			APIVersion: version,
			ClientID:   &clientID,
		}

		mockey.Mock((*RequestHeader).headerVersion).Return(1).Build()

		payload := make([]byte, original.length())
		n := original.Encode(payload)
		payload = payload[:n]

		decoded, nn := DecodeRequestHeader(payload)
		convey.So(nn, convey.ShouldEqual, n)
		convey.So(decoded, convey.ShouldResemble, original)
	})
}

func TestReqeustHeaderEncodeV2(t *testing.T) {
	mockey.PatchConvey("Test RequestHeader v2 Encode", t, func() {
		var version uint16 = 7
		var clientID string = "healer"
		original := RequestHeader{
			APIKey:     API_MetadataRequest,
			APIVersion: version,
			ClientID:   &clientID,
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

		mockey.Mock((*RequestHeader).headerVersion).Return(2).Build()

		payload := make([]byte, original.length())
		n := original.Encode(payload)
		payload = payload[:n]

		decoded, nn := DecodeRequestHeader(payload)
		convey.So(nn, convey.ShouldEqual, n)
		convey.So(decoded, convey.ShouldResemble, original)
	})
}
