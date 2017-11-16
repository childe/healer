package healer

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

// GetOffset return the offset values array from server
func GetOffset(broker string, topic string, partitionID int32, correlationID uint32, clientID string, timeValue int64, offsets uint32) (*OffsetsResponse, error) {
	requestHeader := &RequestHeader{
		ApiKey:        API_OffsetRequest,
		ApiVersion:    0,
		CorrelationID: correlationID,
		ClientId:      clientID,
	}

	partitionOffsetRequestInfos := make(map[uint32]*PartitionOffsetRequestInfo)
	partitionOffsetRequestInfos[uint32(partitionID)] = &PartitionOffsetRequestInfo{
		Time:               timeValue,
		MaxNumberOfOffsets: offsets,
	}
	topicOffsetRequestInfos := make(map[string]map[uint32]*PartitionOffsetRequestInfo)
	topicOffsetRequestInfos[topic] = partitionOffsetRequestInfos

	offsetsRequest := &OffsetsRequest{
		RequestHeader: requestHeader,
		ReplicaId:     -1,
		RequestInfo:   topicOffsetRequestInfos,
	}

	payload := offsetsRequest.Encode()

	dialer := net.Dialer{
		Timeout:   time.Second * 5,
		KeepAlive: time.Hour * 2,
	}

	conn, connErr := dialer.Dial("tcp", broker)
	if connErr != nil {
		//logger.Println(connErr)
		return nil, connErr
	}
	conn.Write(payload)

	responseLengthBuf := make([]byte, 4)
	_, err := conn.Read(responseLengthBuf)
	if err != nil {
		return nil, err
	}

	responseLength := int(binary.BigEndian.Uint32(responseLengthBuf))
	responseBuf := make([]byte, 4+responseLength)

	readLength := 0
	for {
		length, err := conn.Read(responseBuf[4+readLength:])
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		readLength += length
		if readLength > responseLength {
			return nil, fmt.Errorf("fetch more data than needed while read getMetaData response")
		}
	}

	if readLength != responseLength {
		return nil, fmt.Errorf("do NOT fetch needed length while read getMetaData response")
	}
	copy(responseBuf[0:4], responseLengthBuf)

	offsetsResponse, err := NewOffsetsResponse(responseBuf)
	if err != nil {
		return nil, err
	}

	return offsetsResponse, nil
}
