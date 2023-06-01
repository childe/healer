package healer

import (
	"encoding/binary"
	"fmt"
)

// Coordinator is the struct of coordinator, including nodeID, host and port
type Coordinator struct {
	NodeID int32
	Host   string
	Port   int32
}

// FindCoordinatorResponse is the response of findcoordinator request, including correlationID, errorCode, coordinator
type FindCoordinatorResponse struct {
	CorrelationID uint32
	ErrorCode     int16
	Coordinator   Coordinator
}

func (r FindCoordinatorResponse) Error() error {
	return getErrorFromErrorCode(r.ErrorCode)
}

// NewFindCoordinatorResponse create a NewFindCoordinatorResponse instance from response payload bytes
func NewFindCoordinatorResponse(payload []byte, version uint16) (r FindCoordinatorResponse, err error) {
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("FindCoordinator Response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	r.Coordinator.NodeID = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	hostLength := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	r.Coordinator.Host = string(payload[offset : offset+hostLength])
	offset += hostLength

	r.Coordinator.Port = int32(binary.BigEndian.Uint32(payload[offset:]))

	return r, nil
}
