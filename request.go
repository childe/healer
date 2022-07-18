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
	API_FetchRequest: {0},
}

// RequestHeader is the request header, which is used in all requests. It contains apiKey, apiVersion, correlationID, clientID
type RequestHeader struct {
	APIKey        uint16
	APIVersion    uint16
	CorrelationID uint32
	ClientID      string
}

func (requestHeader *RequestHeader) length() int {
	return 10 + len(requestHeader.ClientID)
}

// Encode encodes request header to []byte. this is used the all request
// If the playload is too small, Encode will panic.
func (requestHeader *RequestHeader) Encode(payload []byte) int {
	offset := 0
	binary.BigEndian.PutUint16(payload[offset:], requestHeader.APIKey)
	offset += 2

	binary.BigEndian.PutUint16(payload[offset:], requestHeader.APIVersion)
	offset += 2

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestHeader.CorrelationID))
	offset += 4

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(requestHeader.ClientID)))
	offset += 2
	copy(payload[offset:], requestHeader.ClientID)
	offset += len(requestHeader.ClientID)

	return offset
}

// API returns APiKey of the request(which hold the request header)
func (requestHeader *RequestHeader) API() uint16 {
	return requestHeader.APIKey
}

// Version returns API version of the request
func (requestHeader *RequestHeader) Version() uint16 {
	return requestHeader.APIVersion
}

// SetCorrelationID set request's correlationID
func (requestHeader *RequestHeader) SetCorrelationID(c uint32) {
	requestHeader.CorrelationID = c
}

// Request is implemented by all detailed request
type Request interface {
	Encode(version uint16) []byte
	API() uint16
	SetCorrelationID(uint32)
}
