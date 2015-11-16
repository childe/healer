package gokafka

import (
	"encoding/binary"
)

//var Non-user facing control APIs=4-7
var API_ProduceRequest uint16 = 0
var API_FetchRequest uint16 = 1
var API_OffsetRequest uint16 = 2
var API_MetadataRequest uint16 = 3
var API_OffsetCommitRequest = 8
var API_OffsetFetchRequest = 9
var API_ConsumerMetadataRequest = 10

type RequestHeader struct {
	ApiKey        uint16
	ApiVersion    uint16
	CorrelationId uint32
	ClientId      string
}

func (requestHeader *RequestHeader) Encode(payload []byte, offset int) int {
	binary.BigEndian.PutUint16(payload[offset:], requestHeader.ApiKey)
	offset += 2

	binary.BigEndian.PutUint16(payload[offset:], requestHeader.ApiVersion)
	offset += 2

	binary.BigEndian.PutUint32(payload[offset:], requestHeader.CorrelationId)
	offset += 4

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(requestHeader.ClientId)))
	offset += 2
	copy(payload[offset:], requestHeader.ClientId)
	offset += len(requestHeader.ClientId)

	return offset
}
