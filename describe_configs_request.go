package healer

import "encoding/binary"

// version0
type DescribeConfigsRequest struct {
	RequestHeader *RequestHeader
	Resources     []*DescribeConfigsRequestResource
}

type DescribeConfigsRequestResource struct {
	ResourceType uint8
	ResourceName string
	ConfigNames  []string
}

func (r *DescribeConfigsRequestResource) encode(payload []byte) (offset int) {
	payload[offset] = r.ResourceType
	offset += 1

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.ResourceName)))
	offset += 2

	offset += copy(payload[offset:], r.ResourceName)

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.ConfigNames)))
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
		ApiKey:     API_DescribeConfigs,
		ApiVersion: 0,
		ClientId:   clientID,
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
		l += 1
		l += 2 + len(resource.ResourceName)

		l += 4
		for _, c := range resource.ConfigNames {
			l += 2 + len(c)
		}
	}
	return l
}

func (r *DescribeConfigsRequest) Encode() []byte {
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

func (req *DescribeConfigsRequest) API() uint16 {
	return req.RequestHeader.ApiKey
}

func (req *DescribeConfigsRequest) SetCorrelationID(c uint32) {
	req.RequestHeader.CorrelationID = c
}
