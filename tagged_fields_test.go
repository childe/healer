package healer

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func mockTaggedFields(isFlexible bool) TaggedFields {
	if isFlexible {
		taggedField := TaggedField{Tag: 1, Data: []byte{1, 2, 3, 4, 5}}
		return TaggedFields{taggedField, taggedField}
	}
	return nil
}

func TestTaggedFields(t *testing.T) {
	convey.Convey("Test TaggedFields Encode and Decode", t, func() {
		taggedFields := TaggedFields{
			{Tag: 1, Data: []byte{1, 2, 3}},
			{Tag: 2, Data: []byte{4, 5, 6}},
		}

		encoded := taggedFields.Encode()

		decoded, length := DecodeTaggedFields(encoded)

		convey.So(length, convey.ShouldEqual, len(encoded))
		convey.So(decoded, convey.ShouldResemble, taggedFields)
	})
}

func TestTaggedFieldsNil(t *testing.T) {
	convey.Convey("Test nil TaggedFields Encode and Decode", t, func() {
		var taggedFields TaggedFields = nil

		encoded := taggedFields.Encode()

		decoded, length := DecodeTaggedFields(encoded)

		convey.So(length, convey.ShouldEqual, len(encoded))
		convey.So(decoded, convey.ShouldResemble, taggedFields)
	})
}
