package healer

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestResponseHeaderEncodeDecode(t *testing.T) {
	convey.Convey("TestResponseHeader_Encode", t, func() {
		var apiKey uint16 = 1
		var apiVersion uint16 = 2
		header := NewResponseHeader(apiKey, apiVersion)
		header.CorrelationID = 12345
		header.TaggedFields = TaggedFields{}

		encoded := header.Encode()

		decoded, offset := DecodeResponseHeader(encoded, header.apiKey, header.apiVersion)

		convey.So(offset, convey.ShouldEqual, len(encoded))
		convey.So(decoded, convey.ShouldNotResemble, header)
	})
}
