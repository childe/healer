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

// Represents a sequence of characters.
// First the length N + 1 is given as an UNSIGNED_VARINT.
// Then N bytes follow which are the UTF-8 encoding of the character sequence.
// A null string is represented with a length of 0.
func compactNullableString(payload []byte) (r string, offset int) {
	length, o := binary.Uvarint(payload)
	if length == 0 {
		return "", o
	}
	length--
	r = string(payload[o : o+int(length)])
	return r, o + int(length)
}

// Represents a raw sequence of bytes.
// First the length N+1 is given as an UNSIGNED_VARINT.
// Then N bytes follow.
func compactBytes(payload []byte) (r []byte, offset int) {
	length, o := binary.Uvarint(payload)
	if length == 0 {
		return nil, o
	}
	length--
	r = payload[o : o+int(length)]
	return r, o + int(length)
}

func compactArrayLength(payload []byte) (length uint64, offset int) {
	length, offset = binary.Uvarint(payload)
	length--
	return
}
