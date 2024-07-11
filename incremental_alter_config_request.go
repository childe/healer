package healer

import (
	"encoding/binary"
	"fmt"
)

// IncrementalAlterConfigsRequest struct holds params in AlterConfigsRequest
type IncrementalAlterConfigsRequest struct {
	*RequestHeader
	Resources    []IncrementalAlterConfigsRequestResource `json:"resources"`
	ValidateOnly bool                                     `json:"validate_only"`
}

// IncrementalAlterConfigsRequestResource is sub struct in AlterConfigsRequest
type IncrementalAlterConfigsRequestResource struct {
	ResourceType uint8                                       `json:"type"`
	ResourceName string                                      `json:"name"`
	Entries      []IncrementalAlterConfigsRequestConfigEntry `json:"entries"`
}

func (r IncrementalAlterConfigsRequestResource) encode(payload []byte) (offset int) {
	payload[0] = r.ResourceType
	offset++

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.ResourceName)))
	offset += 2

	offset += copy(payload[offset:], r.ResourceName)

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.Entries)))
	offset += 4

	for _, c := range r.Entries {
		offset += c.encode(payload[offset:])
	}

	return
}

// IncrementalAlterConfigsRequestConfigEntry is sub struct in AlterConfigsRequestResource
type IncrementalAlterConfigsRequestConfigEntry struct {
	Name      string `json:"name"`
	Operation int8   `json:"operation"`
	Value     string `json:"value"`
}

func (c IncrementalAlterConfigsRequestConfigEntry) encode(payload []byte) (offset int) {
	binary.BigEndian.PutUint16(payload[offset:], uint16(len(c.Name)))
	offset += 2

	offset += copy(payload[offset:], c.Name)

	payload[offset] = byte(c.Operation)
	offset++

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(c.Value)))
	offset += 2

	offset += copy(payload[offset:], c.Value)

	return
}

// NewIncrementalAlterConfigsRequest create a new IncrementalAlterConfigsRequest
func NewIncrementalAlterConfigsRequest(clientID string) IncrementalAlterConfigsRequest {
	requestHeader := &RequestHeader{
		APIKey:     API_IncrementalAlterConfigs,
		APIVersion: 0,
		ClientID:   clientID,
	}
	return IncrementalAlterConfigsRequest{requestHeader, nil, false}
}

// SetValidateOnly set validateOnly in request
func (r *IncrementalAlterConfigsRequest) SetValidateOnly(validateOnly bool) *IncrementalAlterConfigsRequest {
	r.ValidateOnly = validateOnly
	return r
}

// AddConfig add new config entry to request
func (r *IncrementalAlterConfigsRequest) AddConfig(resourceType uint8, resourceName, configName, configValue string) error {
	for i, res := range r.Resources {
		if res.ResourceType == resourceType && res.ResourceName == resourceName {
			for _, c := range res.Entries {
				if c.Name == configName {
					if c.Value != configValue {
						return fmt.Errorf("config %s already exist with different value", configName)
					}
					return nil
				}
			}
			e := IncrementalAlterConfigsRequestConfigEntry{
				Name:      configName,
				Operation: 0,
				Value:     configValue,
			}
			res.Entries = append(res.Entries, e)
			r.Resources[i] = res
			return nil
		}
	}
	r.Resources = append(r.Resources, IncrementalAlterConfigsRequestResource{
		ResourceType: resourceType,
		ResourceName: resourceName,
		Entries: []IncrementalAlterConfigsRequestConfigEntry{
			{
				Name:      configName,
				Operation: 0,
				Value:     configValue,
			},
		},
	})
	return nil
}

func (r IncrementalAlterConfigsRequest) length() int {
	l := r.RequestHeader.length()

	l += 4
	for _, resource := range r.Resources {
		l++
		l += 2 + len(resource.ResourceName)

		l += 4
		for _, configEntry := range resource.Entries {
			l += 2 + len(configEntry.Name)
			l++
			l += 2 + len(configEntry.Value)
		}
	}
	l++
	return l
}

// Encode encodes AlterConfigsRequest object to []byte. it implement Request Interface
func (r IncrementalAlterConfigsRequest) Encode(version uint16) []byte {
	requestLength := r.length()

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset += r.RequestHeader.Encode(payload[offset:])

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.Resources)))
	offset += 4

	for _, r := range r.Resources {
		offset += r.encode(payload[offset:])
	}

	if r.ValidateOnly {
		payload[offset] = 1
	} else {
		payload[offset] = 0
	}
	return payload
}

// DecodeIncrementalAlterConfigsRequest decodes []byte to IncrementalAlterConfigsRequest, just used in test cases
func DecodeIncrementalAlterConfigsRequest(payload []byte, version uint16) (r IncrementalAlterConfigsRequest) {
	header, offset := DecodeRequestHeader(payload[4:])
	r.RequestHeader = &header
	offset += 4

	count := binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.Resources = make([]IncrementalAlterConfigsRequestResource, count)
	for i := range r.Resources {
		r.Resources[i].ResourceType = payload[offset]
		offset++
		resourceNameLength := binary.BigEndian.Uint16(payload[offset:])
		offset += 2
		r.Resources[i].ResourceName = string(payload[offset : offset+int(resourceNameLength)])
		offset += int(resourceNameLength)

		entryCount := binary.BigEndian.Uint32(payload[offset:])
		offset += 4
		r.Resources[i].Entries = make([]IncrementalAlterConfigsRequestConfigEntry, entryCount)
		for j := range r.Resources[i].Entries {
			nameLength := binary.BigEndian.Uint16(payload[offset:])
			offset += 2
			r.Resources[i].Entries[j].Name = string(payload[offset : offset+int(nameLength)])
			offset += int(nameLength)

			r.Resources[i].Entries[j].Operation = int8(payload[offset])
			offset++

			valueLength := binary.BigEndian.Uint16(payload[offset:])
			offset += 2
			r.Resources[i].Entries[j].Value = string(payload[offset : offset+int(valueLength)])
			offset += int(valueLength)
		}
	}
	r.ValidateOnly = payload[offset] == 1
	return r
}
