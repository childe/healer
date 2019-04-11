package healer

import (
	"encoding/binary"
	"fmt"
)

// version 0
type DescribeConfigsResponse struct {
	CorrelationID  uint32
	ThrottleTimeMS uint32
	Resources      []*DescribeConfigsResponseResource
}

type DescribeConfigsResponseResource struct {
	ErrorCode     int16
	ErrorMessage  string
	ResourceType  uint8
	ResourceName  string
	ConfigEntries []*DescribeConfigsResponseConfigEntry
}

func (r *DescribeConfigsResponseResource) decode(payload []byte) (offset int) {
	var (
		l int
	)

	r.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	if r.ErrorCode != 0 {
		l = int(int16(binary.BigEndian.Uint16(payload[offset:])))
		offset += 2
		if l > 0 {
			r.ErrorMessage = string(payload[offset : offset+l])
			offset += l
		}
	} else {
		offset += 2
	}

	r.ResourceType = uint8(payload[offset])
	offset += 1

	l = int(int16(binary.BigEndian.Uint16(payload[offset:])))
	offset += 2
	if l >= 0 {
		r.ResourceName = string(payload[offset : offset+l])
		offset += l
	}

	l = int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 4

	r.ConfigEntries = make([]*DescribeConfigsResponseConfigEntry, l)
	for _, c := range r.ConfigEntries {
		c = &DescribeConfigsResponseConfigEntry{}
		offset += c.decode(payload[offset:])
	}

	return
}

type DescribeConfigsResponseConfigEntry struct {
	ConfigName  string
	ConfigValue string
	ReadOnly    bool
	IsDefault   bool
	IsSensitive bool
}

func (c *DescribeConfigsResponseConfigEntry) decode(payload []byte) (offset int) {
	var (
		l int
	)

	l = int(binary.BigEndian.Uint16(payload))
	offset += 2
	c.ConfigName = string(payload[offset : offset+l])
	offset += l

	l = int(binary.BigEndian.Uint16(payload))
	offset += 2
	c.ConfigValue = string(payload[offset : offset+l])
	offset += l

	c.ReadOnly = payload[offset] != 0
	offset += 1

	c.IsDefault = payload[offset] != 0
	offset += 1

	c.IsSensitive = payload[offset] != 0
	offset += 1

	return
}

// TODO only return first error
func NewDescribeConfigsResponse(payload []byte) (*DescribeConfigsResponse, error) {
	var (
		err    error = nil
		offset int   = 0
	)
	r := &DescribeConfigsResponse{}
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return nil, fmt.Errorf("describe_config reseponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.ThrottleTimeMS = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	count := binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.Resources = make([]*DescribeConfigsResponseResource, count)
	for _, r := range r.Resources {
		r = &DescribeConfigsResponseResource{}
		offset += r.decode(payload[offset:])
	}

	return r, err
}
