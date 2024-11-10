package healer

import (
	"encoding/binary"
)

type TaggedFields []TaggedField

type TaggedField struct {
	Tag  int
	Data []byte
}

func (r *TaggedFields) length() (l int) {
	if r == nil {
		return 1
	}
	l = binary.MaxVarintLen64
	for _, v := range *r {
		l += v.length()
	}
	return
}

func (r TaggedFields) Encode() []byte {
	if r == nil {
		return []byte{0}
	}
	payload := make([]byte, r.length())
	offset := binary.PutUvarint(payload, uint64(len(r)))

	for _, v := range r {
		offset += v.encode(payload[offset:])
	}
	return payload[:offset]
}

func DecodeTaggedFields(payload []byte) (r TaggedFields, length int) {
	offset := 0
	taggedFieldsLength, n := binary.Uvarint(payload)
	offset += n
	for i := uint64(0); i < taggedFieldsLength; i++ {
		tag, n := binary.Uvarint(payload[offset:])
		offset += n
		dataLength, n := binary.Uvarint(payload[offset:])
		offset += n
		data := payload[offset : offset+int(dataLength)]
		offset += int(dataLength)
		r = append(r, TaggedField{Tag: int(tag), Data: data})
	}
	return r, offset
}

func (r *TaggedField) length() int {
	return binary.MaxVarintLen32 + +binary.MaxVarintLen32 + len(r.Data)
}

func (r *TaggedField) encode(payload []byte) int {
	offset := binary.PutUvarint(payload, uint64(r.Tag))
	offset += binary.PutUvarint(payload[offset:], uint64(len(r.Data)))
	offset += copy(payload[offset:], r.Data)
	return offset
}
