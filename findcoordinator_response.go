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

type FindCoordinatorReseponse struct {
	CorrelationId uint32
	ErrorCode     uint16
	Coordinator   *Coordinator
}

func (findCoordinatorReseponse *FindCoordinatorReseponse) Decode(payload []byte) error {
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return fmt.Errorf("FindCoordinator Reseponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	findCoordinatorReseponse.CorrelationId = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	findCoordinatorReseponse.ErrorCode = binary.BigEndian.Uint16(payload[offset:])
	offset += 2

	coordinator := &Coordinator{}
	findCoordinatorReseponse.Coordinator = coordinator

	coordinator.nodeID = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	hostLength := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	coordinator.host = string(payload[offset : offset+hostLength])
	offset += hostLength

	coordinator.port = int32(binary.BigEndian.Uint32(payload[offset:]))

	return nil
}
