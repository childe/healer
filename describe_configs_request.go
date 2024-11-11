package healer

import "encoding/binary"

// ConvertConfigResourceType convert string to uint8 that's used in DescribeConfigsRequest
func ConvertConfigResourceType(resourceType string) uint8 {
	switch resourceType {
	case "topic":
		return 2
	case "broker":
		return 4
	case "broker_logger":
		return 8
	default:
		return 0
	}
}

// DescribeConfigsRequest holds the request parameters for DescribeConfigsRequest
type DescribeConfigsRequest struct {
	*RequestHeader
	Resources []*DescribeConfigsRequestResource
}

// DescribeConfigsRequestResource is part of DescribeConfigsRequest
type DescribeConfigsRequestResource struct {
	ResourceType uint8
	ResourceName string
	ConfigNames  []string
}

func (r *DescribeConfigsRequestResource) encode(payload []byte) (offset int) {
	payload[offset] = r.ResourceType
	offset++

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.ResourceName)))
	offset += 2

	offset += copy(payload[offset:], r.ResourceName)

	if r.ConfigNames == nil {
		binary.BigEndian.PutUint32(payload[offset:], 0xffffffff)
	} else {
		binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.ConfigNames)))
	}
	offset += 4

	for _, configName := range r.ConfigNames {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(configName)))
		offset += 2

		offset += copy(payload[offset:], configName)
	}

	return
}

func NewDescribeConfigsRequest(clientID string, resources []*DescribeConfigsRequestResource) *DescribeConfigsRequest {
	requestHeader := &RequestHeader{
		APIKey:     API_DescribeConfigs,
		APIVersion: 0,
		ClientID:   &clientID,
	}
	if resources == nil {
		resources = make([]*DescribeConfigsRequestResource, 0)
	}
	return &DescribeConfigsRequest{requestHeader, resources}
}

func (r *DescribeConfigsRequest) Length() int {
	l := r.RequestHeader.length()

	l += 4
	for _, resource := range r.Resources {
		l++
		l += 2 + len(resource.ResourceName)

		l += 4
		for _, c := range resource.ConfigNames {
			l += 2 + len(c)
		}
	}
	return l
}

func (r *DescribeConfigsRequest) Encode(version uint16) []byte {
	requestLength := r.Length()

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset += r.RequestHeader.EncodeTo(payload[offset:])

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.Resources)))
	offset += 4

	for _, r := range r.Resources {
		offset += r.encode(payload[offset:])
	}

	return payload
}
