package healer

import (
	"encoding/binary"
	"fmt"
)

type ApiVersions struct {
	apiKey     int16
	minVersion int16
	maxVersion int16
}

// version 0
type ApiVersionsResponse struct {
	CorrelationId uint32
	ErrorCode     uint16
	ApiVersions   ApiVersions
}

func (apiVersionsResponse *ApiVersionsResponse) Decode(payload []byte) error {
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return fmt.Errorf("ApiVersions reseponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	apiVersionsResponse.CorrelationId = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	apiVersionsResponse.ErrorCode = binary.BigEndian.Uint16(payload[offset:])
	offset += 2

	apiVersionsResponse.ApiVersions.apiKey = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	apiVersionsResponse.ApiVersions.minVersion = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	apiVersionsResponse.ApiVersions.maxVersion = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	return nil
}
