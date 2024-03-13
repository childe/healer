package healer

import (
	"encoding/binary"
	"fmt"
)

type AlterConfigsResponse struct {
	CorrelationID  uint32
	ThrottleTimeMS uint32
	Resources      []AlterConfigsResponseResource
}

func (r AlterConfigsResponse) Error() error {
	for _, resource := range r.Resources {
		if resource.ErrorCode != 0 {
			return KafkaError(resource.ErrorCode)
		}
	}
	return nil
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
	offset++

	l = int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	r.ResourceName = string(payload[offset : offset+l])
	offset += l
	return
}

func NewAlterConfigsResponse(payload []byte) (r AlterConfigsResponse, err error) {
	var (
		offset int = 0
	)
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("alterconfig response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.ThrottleTimeMS = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	count := binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.Resources = make([]AlterConfigsResponseResource, count)
	for i := range r.Resources {
		rr := &r.Resources[i]
		offset += rr.decode(payload[offset:])
	}

	return r, err
}
