package healer

import "encoding/binary"

/***
https://kafka.apache.org/protocol#protocol_types
some types, such as boolean or int32, are easy to be decoded. I don't write them here.
I only write the types that are not easy to be decoded, including COMPACT_STRING and COMPACT_BYTES etc.
***/

// Represents a sequence of characters.
// First the length N + 1 is given as an UNSIGNED_VARINT.
// Then N bytes follow which are the UTF-8 encoding of the character sequence.
func compactString(payload []byte) (r string, offset int) {
	length, o := binary.Uvarint(payload)
	length--
	r = string(payload[o : o+int(length)])
	return r, o + int(length)
}

// Represents a sequence of characters or null.
// For non-null strings, first the length N is given as an INT16.
// Then N bytes follow which are the UTF-8 encoding of the character sequence.
// A null value is encoded with length of -1 and there are no following bytes.
func nullableString(payload []byte) (r *string, offset int) {
	length := int16(binary.BigEndian.Uint16(payload))
	if length == -1 {
		return nil, 4
	}
	s := string(payload[4 : 4+int(length)])
	return &s, 4 + int(length)
}

// Represents a sequence of characters.
// First the length N + 1 is given as an UNSIGNED_VARINT.
// Then N bytes follow which are the UTF-8 encoding of the character sequence.
// A null string is represented with a length of 0.
func compactNullableString(payload []byte) (r *string, offset int) {
	length, o := binary.Uvarint(payload)
	if length == 0 {
		return nil, o
	}
	length--
	s := string(payload[o : o+int(length)])
	return &s, o + int(length)
}

// Represents a raw sequence of bytes.
// First the length N+1 is given as an UNSIGNED_VARINT.
// Then N bytes follow.
func compactBytes(payload []byte) (r []byte, offset int) {
	length, o := binary.Uvarint(payload)
	length--
	r = payload[o : o+int(length)]
	return r, o + int(length)
}

// Represents a raw sequence of bytes or null.
// For non-null values, first the length N is given as an INT32.
// Then N bytes follow.
// A null value is encoded with length of -1 and there are no following bytes.
func nullableBytes(payload []byte) (r []byte, offset int) {
	length := int32(binary.BigEndian.Uint32(payload))
	if length == -1 {
		return nil, 4
	}
	r = payload[4 : 4+int(length)]
	return r, 4 + int(length)
}

// Represents a raw sequence of bytes.
// First the length N+1 is given as an UNSIGNED_VARINT.
// Then N bytes follow. A null object is represented with a length of 0.
func compactNullableBytes(payload []byte) (r []byte, offset int) {
	length, o := binary.Uvarint(payload)
	length--

	if length == 0 {
		return nil, o
	}
	r = payload[o : o+int(length)]
	return r, o + int(length)
}

// Represents a sequence of objects of a given type T.
// Type T can be either a primitive type (e.g.  STRING) or a structure.
// First, the length N + 1 is given as an UNSIGNED_VARINT.
// Then N instances of type T follow.
// A null array is represented with a length of 0.
// In protocol documentation an array of T instances is referred to as [T].
func compactArrayLength(payload []byte) (length uint64, offset int) {
	length, offset = binary.Uvarint(payload)
	return length - 1, offset
}
