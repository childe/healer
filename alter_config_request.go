package healer

import (
	"encoding/binary"
	"fmt"
)

// AlterConfigsRequest struct holds params in AlterConfigsRequest
type AlterConfigsRequest struct {
	*RequestHeader
	Resources []AlterConfigsRequestResource
}

// AlterConfigsRequestResource is sub struct in AlterConfigsRequest
type AlterConfigsRequestResource struct {
	ResourceType  uint8
	ResourceName  string
	ConfigEntries []AlterConfigsRequestConfigEntry
}

func (r *AlterConfigsRequestResource) encode(payload []byte) (offset int) {
	payload[0] = r.ResourceType
	offset++

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.ResourceName)))
	offset += 2

	offset += copy(payload[offset:], r.ResourceName)

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.ConfigEntries)))
	offset += 4

	for _, c := range r.ConfigEntries {
		offset += c.encode(payload[offset:])
	}

	return
}

// AlterConfigsRequestConfigEntry is sub struct in AlterConfigsRequestResource
type AlterConfigsRequestConfigEntry struct {
	ConfigName  string
	ConfigValue string
}

func (c *AlterConfigsRequestConfigEntry) encode(payload []byte) (offset int) {
	binary.BigEndian.PutUint16(payload[offset:], uint16(len(c.ConfigName)))
	offset += 2

	offset += copy(payload[offset:], c.ConfigName)

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(c.ConfigValue)))
	offset += 2

	offset += copy(payload[offset:], c.ConfigValue)

	return
}

// NewAlterConfigsRequest create a new AlterConfigsRequest
func NewAlterConfigsRequest(clientID string) *AlterConfigsRequest {
	requestHeader := &RequestHeader{
		APIKey:     API_AlterConfigs,
		APIVersion: 0,
		ClientID:   clientID,
	}
	return &AlterConfigsRequest{requestHeader, nil}
}

// AddConfig add new config entry to request
func (r *AlterConfigsRequest) AddConfig(resourceType uint8, resourceName, configName, configValue string) error {
	for i, res := range r.Resources {
		if res.ResourceType == res.ResourceType && res.ResourceName == resourceName {
			for _, c := range res.ConfigEntries {
				if c.ConfigName == configName {
					if c.ConfigValue != c.ConfigValue {
						return fmt.Errorf("config %s already exist with different value", configName)
					}
					return nil
				}
			}
			e := AlterConfigsRequestConfigEntry{
				ConfigName:  configName,
				ConfigValue: configValue,
			}
			res.ConfigEntries = append(res.ConfigEntries, e)
			r.Resources[i] = res
			return nil
		}
	}
	r.Resources = append(r.Resources, AlterConfigsRequestResource{
		ResourceType: resourceType,
		ResourceName: resourceName,
		ConfigEntries: []AlterConfigsRequestConfigEntry{
			{
				ConfigName:  configName,
				ConfigValue: configValue,
			},
		},
	})
	return nil
}

func (r *AlterConfigsRequest) length() int {
	l := r.RequestHeader.length()

	l += 4
	for _, resource := range r.Resources {
		l++
		l += 2 + len(resource.ResourceName)

		l += 4
		for _, configEntry := range resource.ConfigEntries {
			l += 2 + len(configEntry.ConfigName)
			l += 2 + len(configEntry.ConfigValue)
		}
	}
	return l
}

// Encode encodes AlterConfigsRequest object to []byte. it implement Request Interface
func (r *AlterConfigsRequest) Encode(version uint16) []byte {
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

	return payload
}
