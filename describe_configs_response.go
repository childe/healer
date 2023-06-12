package healer

import (
	"encoding/binary"
	"fmt"
)

// DescribeConfigsResponse holds the parameters of a describe-configs response.
type DescribeConfigsResponse struct {
	CorrelationID  uint32
	ThrottleTimeMS uint32
	Resources      []describeConfigsResponseResource
}

// FIXME: multiple error code
func (r DescribeConfigsResponse) Error() error {
	for _, resource := range r.Resources {
		if resource.ErrorCode != 0 {
			return fmt.Errorf("describe configs failed: %s", getErrorFromErrorCode(resource.ErrorCode))
		}
	}
	return nil
}

type describeConfigsResponseResource struct {
	ErrorCode     int16
	ErrorMessage  string
	ResourceType  uint8
	ResourceName  string
	ConfigEntries []describeConfigsResponseConfigEntry
}

func decodeToDescribeConfigsResponseResource(payload []byte) (r describeConfigsResponseResource, offset int) {
	var l int

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
	offset++

	l = int(int16(binary.BigEndian.Uint16(payload[offset:])))
	offset += 2
	if l >= 0 {
		r.ResourceName = string(payload[offset : offset+l])
		offset += l
	}

	count := int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.ConfigEntries = make([]describeConfigsResponseConfigEntry, count)
	for i := range r.ConfigEntries {
		c, o := decodeToDescribeConfigsResponseConfigEntry(payload[offset:])
		r.ConfigEntries[i] = c
		offset += o
	}

	return
}

type describeConfigsResponseConfigEntry struct {
	ConfigName  string
	ConfigValue string
	ReadOnly    bool
	IsDefault   bool
	IsSensitive bool
}

func decodeToDescribeConfigsResponseConfigEntry(payload []byte) (r describeConfigsResponseConfigEntry, offset int) {
	l := int(binary.BigEndian.Uint16(payload))
	offset += 2
	r.ConfigName = string(payload[offset : offset+l])
	offset += l

	l = int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	r.ConfigValue = string(payload[offset : offset+l])
	offset += l

	r.ReadOnly = payload[offset] != 0
	offset++

	r.IsDefault = payload[offset] != 0
	offset++

	r.IsSensitive = payload[offset] != 0
	offset++

	return
}

// NewDescribeConfigsResponse creates a new DescribeConfigsResponse from the given payload
func NewDescribeConfigsResponse(payload []byte) (r DescribeConfigsResponse, err error) {
	var (
		offset int = 0
	)
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("describe_configs response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.ThrottleTimeMS = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	count := binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.Resources = make([]describeConfigsResponseResource, count)
	for i := range r.Resources {
		c, o := decodeToDescribeConfigsResponseResource(payload[offset:])
		r.Resources[i] = c
		offset += o
	}

	return r, err
}
