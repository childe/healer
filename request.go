package healer

import (
	"encoding/binary"
)

// TODO type define ApiKey and change api_XXX to ApiKey type

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
	API_SaslHandshake       uint16 = 17
	API_ApiVersions         uint16 = 18
	API_CreateTopics        uint16 = 19
	API_DescribeConfigs     uint16 = 32
	API_AlterConfigs        uint16 = 33
	API_SaslAuthenticate    uint16 = 36
)

var availableVersions map[uint16][]uint16 = map[uint16][]uint16{
	API_FetchRequest: []uint16{10, 0},
}

type RequestHeader struct {
	ApiKey        uint16
	ApiVersion    uint16
	CorrelationID uint32
	ClientId      string
}

func (requestHeader *RequestHeader) length() int {
	return 10 + len(requestHeader.ClientId)
}

// Encode encodes request header to []byte. this is used the all detailed request
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

// API returns APiKey of the request(which hold the request header)
func (requestHeader *RequestHeader) API() uint16 {
	return requestHeader.ApiKey
}

// APIVersion returns API version of the request
func (requestHeader *RequestHeader) APIVersion() uint16 {
	return requestHeader.ApiVersion
}

// SetCorrelationID set request's correlationID
func (requestHeader *RequestHeader) SetCorrelationID(c uint32) {
	requestHeader.CorrelationID = c
}

// Request is implemented by all detailed request
type Request interface {
	Encode() []byte
	API() uint16
	SetCorrelationID(uint32)
}
