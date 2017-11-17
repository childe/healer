package healer

import (
	"encoding/binary"
)

//var Non-user facing control APIs=4-7
var (
	API_ProduceRequest      uint16 = 0
	API_FetchRequest        uint16 = 1
	API_OffsetRequest       uint16 = 2
	API_MetadataRequest     uint16 = 3
	API_OffsetCommitRequest uint16 = 8
	API_OffsetFetchRequest  uint16 = 9
	API_FindCoordinator     uint16 = 10
	API_JoinGroup           uint16 = 11
	API_Heartbeat           uint16 = 12
	API_LeaveGroup          uint16 = 13
	API_SyncGroup           uint16 = 14
	API_DescribeGroups      uint16 = 15
	API_ListGroups          uint16 = 16
	API_ApiVersions         uint16 = 18
)

type RequestHeader struct {
	ApiKey        uint16
	ApiVersion    uint16
	CorrelationID uint32
	ClientId      string
}

func (requestHeader *RequestHeader) length() int {
	return 10 + len(requestHeader.ClientId)
}

func (requestHeader *RequestHeader) Encode(payload []byte, offset int) int {
	binary.BigEndian.PutUint16(payload[offset:], requestHeader.ApiKey)
	offset += 2

	binary.BigEndian.PutUint16(payload[offset:], requestHeader.ApiVersion)
	offset += 2

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestHeader.CorrelationID))
	offset += 4

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(requestHeader.ClientId)))
	offset += 2
	copy(payload[offset:], requestHeader.ClientId)
	offset += len(requestHeader.ClientId)

	return offset
}

type Request interface {
	Encode() []byte
}
