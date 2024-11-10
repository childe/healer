package healer

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// AlterPartitionReassignmentsResponse is the response of AlterPartitionReassignmentsRequest
type AlterPartitionReassignmentsResponse struct {
	ResponseHeader
	ThrottleTimeMs int32                                       `json:"throttle_time_ms"`
	ErrorCode      int16                                       `json:"error_code"`
	ErrorMsg       *string                                     `json:"error_msg"`
	Responses      []*alterPartitionReassignmentsResponseTopic `json:"responses"`

	TaggedFields TaggedFields `json:"tagged_fields"`
}

func (r *AlterPartitionReassignmentsResponse) Error() error {
	if r.ErrorCode != 0 {
		return fmt.Errorf("%w: %s", KafkaError(r.ErrorCode), *r.ErrorMsg)
	}
	for _, t := range r.Responses {
		for _, p := range t.Partitions {
			if p.ErrorCode != 0 {
				return fmt.Errorf("%w: %s", KafkaError(p.ErrorCode), *p.ErrorMsg)
			}
		}
	}
	return nil
}

// just for test
func (r *AlterPartitionReassignmentsResponse) Encode(version uint16) (payload []byte) {
	defer func() {
		length := len(payload)
		binary.BigEndian.PutUint32(payload, uint32(length-4))
		payload = payload[:length]
	}()
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, uint32(0)) // length

	buf.Write(r.ResponseHeader.Encode())
	binary.Write(buf, binary.BigEndian, r.ThrottleTimeMs)
	binary.Write(buf, binary.BigEndian, r.ErrorCode)
	writeCompactNullableString(buf, r.ErrorMsg)

	buf.Write(encodeCompactArrayLength(len(r.Responses)))
	for _, t := range r.Responses {
		buf.Write(t.encode())
	}
	buf.Write(r.TaggedFields.Encode())
	return buf.Bytes()
}

// alterPartitionReassignmentsResponseTopic is the response of AlterPartitionReassignmentsRequest
type alterPartitionReassignmentsResponseTopic struct {
	Name       string                                               `json:"name"`
	Partitions []*alterPartitionReassignmentsResponseTopicPartition `json:"partitions"`

	TaggedFields TaggedFields `json:"tagged_fields"`
}

func (r *alterPartitionReassignmentsResponseTopic) encode() []byte {
	buf := &bytes.Buffer{}
	writeCompactString(buf, r.Name)

	buf.Write(encodeCompactArrayLength(len(r.Partitions)))
	for _, p := range r.Partitions {
		buf.Write(p.encode())
	}

	buf.Write(r.TaggedFields.Encode())
	return buf.Bytes()
}

type alterPartitionReassignmentsResponseTopicPartition struct {
	PartitionID int32   `json:"partition_id"`
	ErrorCode   int16   `json:"error_code"`
	ErrorMsg    *string `json:"error_msg"`

	TaggedFields TaggedFields `json:"tagged_fields"`
}

func (r *alterPartitionReassignmentsResponseTopicPartition) encode() []byte {
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, r.PartitionID)
	binary.Write(buf, binary.BigEndian, r.ErrorCode)
	writeCompactNullableString(buf, r.ErrorMsg)
	buf.Write(r.TaggedFields.Encode())
	return buf.Bytes()
}

func decodeToAlterPartitionReassignmentsResponseTopicPartition(payload []byte, version uint16) (p *alterPartitionReassignmentsResponseTopicPartition, length int) {
	p = &alterPartitionReassignmentsResponseTopicPartition{}
	offset := 0
	p.PartitionID = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	p.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	errorMsg, o := compactNullableString(payload[offset:])
	offset += o
	p.ErrorMsg = errorMsg

	taggedFields, n := DecodeTaggedFields(payload[offset:])
	offset += n
	p.TaggedFields = taggedFields

	return p, offset
}

// NewAlterPartitionReassignmentsResponse create a new AlterPartitionReassignmentsResponse
func NewAlterPartitionReassignmentsResponse(payload []byte, version uint16) (*AlterPartitionReassignmentsResponse, error) {
	responseLength := int32(binary.BigEndian.Uint32(payload))
	if int(responseLength)+4 != len(payload) {
		return nil, fmt.Errorf("AlterPartitionReassignments response length did not match: %d!=%d", responseLength+4, len(payload))
	}

	offset := 4
	header, o := DecodeResponseHeader(payload[offset:], API_AlterPartitionReassignments, version)
	offset += o

	r := &AlterPartitionReassignmentsResponse{
		ResponseHeader: header,
	}

	r.ThrottleTimeMs = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	errorMsg, o := compactNullableString(payload[offset:])
	offset += o
	r.ErrorMsg = errorMsg

	responseCount, o := compactArrayLength(payload[offset:])
	offset += o
	if responseCount < 0 { // actually it should be -1
		r.Responses = nil
	} else {
		r.Responses = make([]*alterPartitionReassignmentsResponseTopic, responseCount)
		for i := 0; i < int(responseCount); i++ {
			topic := &alterPartitionReassignmentsResponseTopic{}
			r.Responses[i] = topic

			topic.Name, o = compactString(payload[offset:])
			offset += o

			partitionCount, o := compactArrayLength(payload[offset:])
			offset += o
			if partitionCount <= 0 {
				topic.Partitions = nil
			}

			topic.Partitions = make([]*alterPartitionReassignmentsResponseTopicPartition, partitionCount)
			for j := 0; j < int(partitionCount); j++ {
				topic.Partitions[j], o = decodeToAlterPartitionReassignmentsResponseTopicPartition(payload[offset:], version)
				offset += o
			}

			topic.TaggedFields, o = DecodeTaggedFields(payload[offset:])
			offset += o
		}
	}

	r.TaggedFields, o = DecodeTaggedFields(payload[offset:])
	offset += o

	return r, nil
}
