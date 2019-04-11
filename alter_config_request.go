package healer

import (
	"encoding/binary"
)

// version0
type AlterConfigsRequest struct {
	RequestHeader *RequestHeader
	Resources     []*Resource
}

type Resource struct {
	ResourceType  uint8
	ResourceName  string
	ConfigEntries []*ConfigEntry
}

func (r *Resource) encode(payload []byte) (offset int) {
	payload[0] = r.ResourceType
	offset += 1

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.ResourceName)))
	offset += 2

	copy(payload[offset:], r.ResourceName)
	offset += len(r.ResourceName)

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.ConfigEntries)))
	offset += 4

	for _, c := range r.ConfigEntries {
		offset += c.encode(payload[offset:])
	}

	return
}

type ConfigEntry struct {
	ConfigName  string
	ConfigValue string
}

func (c *ConfigEntry) encode(payload []byte) (offset int) {
	binary.BigEndian.PutUint16(payload[offset:], uint16(len(c.ConfigName)))
	offset += 2

	copy(payload[offset:], c.ConfigName)
	offset += len(c.ConfigName)

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(c.ConfigValue)))
	offset += 2

	copy(payload[offset:], c.ConfigValue)
	offset += len(c.ConfigValue)

	return
}

func NewAlterConfigsRequest(clientID string, resources []*Resource) *AlterConfigsRequest {
	requestHeader := &RequestHeader{
		ApiKey:     API_AlterConfigs,
		ApiVersion: 0,
		ClientId:   clientID,
	}
	return &AlterConfigsRequest{requestHeader, resources}
}

func (r *AlterConfigsRequest) Length() int {
	l := r.RequestHeader.length()

	l += 4
	for _, resource := range r.Resources {
		l += 1
		l += 2 + len(resource.ResourceName)

		l += 4
		for _, configEntry := range resource.ConfigEntries {
			l += 2 + len(configEntry.ConfigName)
			l += 2 + len(configEntry.ConfigValue)
		}
	}
	return l
}

func (r *AlterConfigsRequest) Encode() []byte {
	requestLength := r.Length()

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset = r.RequestHeader.Encode(payload, offset)

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.Resources)))
	offset += 4

	for _, r := range r.Resources {
		offset += r.encode(payload[offset:])
	}

	return payload
}

func (req *AlterConfigsRequest) API() uint16 {
	return req.RequestHeader.ApiKey
}

func (req *AlterConfigsRequest) SetCorrelationID(c uint32) {
	req.RequestHeader.CorrelationID = c
}
