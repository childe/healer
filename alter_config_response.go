package healer

import (
	"encoding/binary"
	"fmt"
)

// version 0
type AlterConfigsResponse struct {
	CorrelationID  uint32
	ThrottleTimeMS uint32
	Resources      []*AlterConfigsResponseResource
}

type AlterConfigsResponseResource struct {
	ErrorCode    int16
	ErrorMessage string
	ResourceType uint8
	ResourceName string
}

func (r *AlterConfigsResponseResource) decode(payload []byte) (resourceLength int) {
	offset := 0
	r.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	l := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	r.ErrorMessage = string(payload[offset : offset+l])
	offset += l

	r.ResourceType = payload[offset]
	offset += 1

	l = int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	r.ResourceName = string(payload[offset : offset+l])
	offset += l
	return
}

// TODO only return first error
func NewAlterConfigsResponse(payload []byte) (*AlterConfigsResponse, error) {
	var (
		err    error = nil
		offset int   = 0
	)
	r := &AlterConfigsResponse{}
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return nil, fmt.Errorf("alterconfig reseponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.ThrottleTimeMS = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	count := binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.Resources = make([]*AlterConfigsResponseResource, count)
	for _, r := range r.Resources {
		offset += r.decode(payload[offset:])
	}

	return r, err
}
