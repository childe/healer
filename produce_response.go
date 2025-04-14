package healer

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
)

type produceResponseRecordErrors struct {
	BatchIndex             int32
	BatchIndexErrorMessage *string

	TaggedField TaggedFields `json:"tagged_fields"`
}
type produceResponseP struct {
	PartitionID int32
	ErrorCode   int16
	BaseOffset  int64

	LogAppendTimeMs int64 `healer:"minVersion:3"`
	LogStartOffset  int64 `healer:"minVersion:5"`

	RecordErrors []produceResponseRecordErrors `healer:"minVersion:8"`

	ErrorMessage *string `healer:"minVersion:8"`

	TaggedField TaggedFields `json:"tagged_fields"`
}

var tagsCacheProduceResponseP atomic.Value

func (g *produceResponseP) tags() (fieldsVersions map[string]uint16) {
	if v := tagsCacheProduceResponseP.Load(); v != nil {
		return v.(map[string]uint16)
	}

	fieldsVersions = healerTags(*g)
	tagsCacheProduceResponseP.Store(fieldsVersions)
	return
}
func (p *produceResponseP) decode(payload []byte, version uint16, isFlexible bool) (offset int) {
	var o int

	p.PartitionID = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	p.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	p.BaseOffset = int64(binary.BigEndian.Uint64(payload[offset:]))
	offset += 8

	tags := p.tags()
	if version >= tags["GroupState"] {
		p.LogAppendTimeMs = int64(binary.BigEndian.Uint64(payload[offset:]))
		offset += 8
	}

	if version >= tags["LogStartOffset"] {
		p.LogStartOffset = int64(binary.BigEndian.Uint64(payload[offset:]))
		offset += 8
	}

	if version >= tags["RecordErrors"] {
		if isFlexible {
			count, o := compactArrayLength(payload[offset:])
			p.RecordErrors = make([]produceResponseRecordErrors, count)
			offset += o
		} else {
			count := binary.BigEndian.Uint32(payload[offset:])
			p.RecordErrors = make([]produceResponseRecordErrors, count)
			offset += 4
		}

		for i := range p.RecordErrors {
			r := &p.RecordErrors[i]
			r.BatchIndex = int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4

			if isFlexible {
				r.BatchIndexErrorMessage, o = compactNullableString(payload[offset:])
				offset += o
			} else {
				r.BatchIndexErrorMessage, o = nullableString(payload[offset:])
				offset += o
			}

			r.TaggedField, o = DecodeTaggedFields(payload[offset:])
			offset += o
		}
	}

	if version >= tags["ErrorMessage"] {
		if isFlexible {
			p.ErrorMessage, o = compactNullableString(payload[offset:])
		} else {
			p.ErrorMessage, o = nullableString(payload[offset:])
		}
		offset += o
	}

	return
}

type produceResponseT struct {
	Topic       string
	Partitions  []produceResponseP
	TaggedField TaggedFields `json:"tagged_fields"`
}

func (t *produceResponseT) decode(payload []byte, version uint16, isFlexible bool) (offset int) {
	var o int
	if isFlexible {
		t.Topic, offset = compactString(payload)
	} else {
		t.Topic, offset = nonnullableString(payload)
	}

	if isFlexible {
		count, o := compactArrayLength(payload[offset:])
		t.Partitions = make([]produceResponseP, count)
		offset += o
	} else {
		count := binary.BigEndian.Uint32(payload[offset:])
		offset += 4
		t.Partitions = make([]produceResponseP, count)
	}
	for j := range t.Partitions {
		p := &t.Partitions[j]
		p.decode(payload[offset:], version, isFlexible)
	}

	t.TaggedField, o = DecodeTaggedFields(payload[offset:])
	offset += o

	return
}

type ProduceResponse struct {
	ResponseHeader
	ProduceResponses []produceResponseT

	ThrottleTimeMs int32        `healer:"minVersion:3"`
	TaggedField    TaggedFields `json:"tagged_fields"`
}

func (r ProduceResponse) Error() error {
	for _, produceResponse := range r.ProduceResponses {
		for _, partition := range produceResponse.Partitions {
			if partition.ErrorCode != 0 {
				return KafkaError(partition.ErrorCode)
			}
		}
	}
	return nil
}

func NewProduceResponse(payload []byte, version uint16) (r ProduceResponse, err error) {
	var (
		offset int
		o      int
	)
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("produce response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.ResponseHeader, o = DecodeResponseHeader(payload[offset:], API_DeleteAcls, version)
	offset += o

	isFlexible := r.ResponseHeader.IsFlexible()

	if isFlexible {
		produceResponseTCount, o := compactArrayLength(payload[offset:])
		r.ProduceResponses = make([]produceResponseT, produceResponseTCount)
		offset += o
	} else {
		produceResponseP := int(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
		r.ProduceResponses = make([]produceResponseT, produceResponseP)
	}

	for i := range r.ProduceResponses {
		offset += r.ProduceResponses[i].decode(payload[offset:], version, isFlexible)
	}

	return r, err
}
