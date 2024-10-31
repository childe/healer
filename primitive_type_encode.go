package healer

import (
	"encoding/binary"
	"io"
)

// https://kafka.apache.org/protocol#protocol_types

// Represents a sequence of characters.
// First the length N + 1 is given as an UNSIGNED_VARINT .
// Then N bytes follow which are the UTF-8 encoding of the character sequence.
func encodeCompactString(s string) []byte {
	payload := make([]byte, len(s)+binary.MaxVarintLen64)
	offset := binary.PutUvarint(payload, 1+uint64(len(s)))
	offset += copy(payload[offset:], s)
	return payload[:offset]
}

func writeCompactString(w io.Writer, s string) (n int, err error) {
	payload := make([]byte, len(s)+binary.MaxVarintLen64)
	offset := binary.PutUvarint(payload, 1+uint64(len(s)))
	if n, err = w.Write(payload[:offset]); err != nil {
		return
	}
	o, err := w.Write([]byte(s))
	return n + o, err
}

// Represents a sequence of characters or null.
// For non-null strings, first the length N is given as an INT16.
// Then N bytes follow which are the UTF-8 encoding of the character sequence.
// A null value is encoded with length of -1 and there are no following bytes.
func encodeNullableString(s *string) []byte {
	if s == nil {
		return binary.BigEndian.AppendUint16(nil, 0xFFFF)
	}

	payload := make([]byte, len(*s)+2)
	binary.BigEndian.PutUint16(payload, uint16(len(*s)))
	offset := copy(payload[2:], *s)
	return payload[:2+offset]
}

func writeNullableString(w io.Writer, s *string) (n int, err error) {
	if s == nil {
		return w.Write(binary.BigEndian.AppendUint16(nil, 0xFFFF))
	}
	payload := make([]byte, len(*s)+2)
	binary.BigEndian.PutUint16(payload, uint16(len(*s)))
	if n, err = w.Write(payload[:2]); err != nil {
		return
	}
	o, err := w.Write([]byte(*s))
	return n + o, err
}

// Represents a sequence of characters.
// First the length N + 1 is given as an UNSIGNED_VARINT .
// Then N bytes follow which are the UTF-8 encoding of the character sequence.
// A null string is represented with a length of 0.
func encodeCompactNullableString(s *string) []byte {
	if s == nil {
		return []byte{0}
	}
	payload := make([]byte, len(*s)+binary.MaxVarintLen16)
	offset := binary.PutUvarint(payload, 1+uint64(len(*s)))
	offset += copy(payload[offset:], *s)
	return payload[:offset]
}

func writeCompactNullableString(w io.Writer, s *string) (n int, err error) {
	if s == nil {
		return w.Write([]byte{0})
	}
	payload := make([]byte, len(*s)+binary.MaxVarintLen16)
	offset := binary.PutUvarint(payload, 1+uint64(len(*s)))
	if n, err = w.Write(payload[:offset]); err != nil {
		return
	}
	o, err := w.Write([]byte(*s))
	return n + o, err
}

// Represents a raw sequence of bytes.
// First the length N+1 is given as an UNSIGNED_VARINT.
// Then N bytes follow.
func encodeCompactBytes(s []byte) []byte {
	payload := make([]byte, len(s)+binary.MaxVarintLen32)
	offset := binary.PutUvarint(payload, 1+uint64(len(s)))
	offset += copy(payload[offset:], s)
	return payload[:offset]
}

// Represents a raw sequence of bytes or null.
// For non-null values, first the length N is given as an INT32.
// Then N bytes follow.
// A null value is encoded with length of -1 and there are no following bytes.
func encodeNullableBytes(s []byte) []byte {
	if s == nil {
		return binary.BigEndian.AppendUint32(nil, 0xFFFFFFFF)
	}
	payload := make([]byte, len(s)+4)
	binary.BigEndian.AppendUint32(payload, uint32(len(s)))
	offset := copy(payload[4:], s)
	return payload[:4+offset]
}

// Represents a raw sequence of bytes.
// First the length N+1 is given as an UNSIGNED_VARINT.
// Then N bytes follow. A null object is represented with a length of 0.
func encodeCompactNullableBytes(s []byte) []byte {
	if s == nil {
		return []byte{0}
	}
	payload := make([]byte, len(s)+binary.MaxVarintLen32)
	offset := binary.PutVarint(payload, 1+int64(len(s)))
	offset += copy(payload[offset:], s)
	return payload[:offset]
}
