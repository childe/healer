package healer

import (
	"encoding/binary"
	"fmt"
)

type Coordinator struct {
	nodeID int32
	host   string
	port   int32
}

type FindCoordinatorResponse struct {
	CorrelationID uint32
	ErrorCode     uint16
	Coordinator   *Coordinator
}

func NewFindCoordinatorResponse(payload []byte) (*FindCoordinatorResponse, error) {
	findCoordinatorResponse := &FindCoordinatorResponse{}
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return nil, fmt.Errorf("FindCoordinator Response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	findCoordinatorResponse.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	findCoordinatorResponse.ErrorCode = binary.BigEndian.Uint16(payload[offset:])
	offset += 2

	coordinator := &Coordinator{}
	findCoordinatorResponse.Coordinator = coordinator

	coordinator.nodeID = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	hostLength := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	coordinator.host = string(payload[offset : offset+hostLength])
	offset += hostLength

	coordinator.port = int32(binary.BigEndian.Uint32(payload[offset:]))

	return findCoordinatorResponse, nil
}
