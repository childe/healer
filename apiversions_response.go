package healer

import (
	"encoding/binary"
	"fmt"
)

type ApiVersion struct {
	apiKey     int16
	minVersion int16
	maxVersion int16
}

// version 0
type ApiVersionsResponse struct {
	CorrelationID uint32
	ErrorCode     uint16
	ApiVersions   []*ApiVersion
}

func NewApiVersionsResponse(payload []byte) (*ApiVersionsResponse, error) {
	apiVersionsResponse := &ApiVersionsResponse{}
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return nil, fmt.Errorf("ApiVersions reseponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	apiVersionsResponse.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	apiVersionsResponse.ErrorCode = binary.BigEndian.Uint16(payload[offset:])
	offset += 2

	apiVersionsCount := binary.BigEndian.Uint32(payload[offset:])
	offset += 4
	apiVersionsResponse.ApiVersions = make([]*ApiVersion, apiVersionsCount)

	for i := uint32(0); i < apiVersionsCount; i++ {
		apiVersion := &ApiVersion{}
		apiVersion.apiKey = int16(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2

		apiVersion.minVersion = int16(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2

		apiVersion.maxVersion = int16(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2

		apiVersionsResponse.ApiVersions[i] = apiVersion
	}

	return apiVersionsResponse, nil
}
