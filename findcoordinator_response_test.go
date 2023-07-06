package healer

import (
	"encoding/binary"
	"reflect"
	"testing"
)

// only for unit test
func (r FindCoordinatorResponse) decode() []byte {
	payload := make([]byte, 0)

	offset := 0
	size := 0

	size = 4
	payload = append(payload, make([]byte, size)...)
	binary.BigEndian.PutUint32(payload[offset:], r.CorrelationID)
	offset += size

	size = 2
	payload = append(payload, make([]byte, size)...)
	binary.BigEndian.PutUint16(payload[offset:], uint16(r.ErrorCode))
	offset += size

	size = 4
	payload = append(payload, make([]byte, size)...)
	binary.BigEndian.PutUint32(payload[offset:], uint32(r.Coordinator.NodeID))
	offset += size

	size = 2
	payload = append(payload, make([]byte, size)...)
	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.Coordinator.Host)))
	offset += size

	size = len(r.Coordinator.Host)
	payload = append(payload, make([]byte, size)...)
	copy(payload[offset:], r.Coordinator.Host)
	offset += size

	size = 4
	payload = append(payload, make([]byte, size)...)
	binary.BigEndian.PutUint32(payload[offset:], uint32(r.Coordinator.Port))
	offset += size

	rst := make([]byte, len(payload)+4)
	binary.BigEndian.PutUint32(rst, uint32(len(payload)))
	copy(rst[4:], payload)
	return rst
}

func TestFindcoordinatorResponseDecode(t *testing.T) {
	r := FindCoordinatorResponse{
		CorrelationID: 1,
		ErrorCode:     0,
		Coordinator: Coordinator{
			NodeID: 4,
			Host:   "127.0.0.1",
			Port:   9092,
		},
	}

	var payload []byte = r.decode()
	nr, err := NewFindCoordinatorResponse(payload, 0)
	if err != nil {
		t.Errorf("FindCoordinatorResponse encode failed: %v", err)
	}
	if reflect.DeepEqual(r, nr) {
		t.Logf("FindCoordinatorResponse encode success")
	} else {
		t.Errorf("FindCoordinatorResponse encode failed. %v!=%v", r, nr)
	}
}
