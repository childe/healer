package gokafka

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

// GetOffset return the offset values array from server
func GetOffset(broker BrokerInfo, topic string, partitionID int32, correlationID int32, clientID string, timeValue int64, offsets uint32) (*OffsetResponse, error) {
	requestHeader := &RequestHeader{
		ApiKey:        API_OffsetRequest,
		ApiVersion:    0,
		CorrelationId: int32(correlationID),
		ClientId:      clientID,
	}

	partitionOffsetRequestInfos := make(map[uint32]*PartitionOffsetRequestInfo)
	partitionOffsetRequestInfos[uint32(partitionID)] = &PartitionOffsetRequestInfo{
		Time:               timeValue,
		MaxNumberOfOffsets: offsets,
	}
	topicOffsetRequestInfos := make(map[string]map[uint32]*PartitionOffsetRequestInfo)
	topicOffsetRequestInfos[topic] = partitionOffsetRequestInfos

	offsetReqeust := &OffsetReqeust{
		RequestHeader: requestHeader,
		ReplicaId:     -1,
		RequestInfo:   topicOffsetRequestInfos,
	}

	payload := offsetReqeust.Encode()

	dialer := net.Dialer{
		Timeout:   time.Second * 5,
		KeepAlive: time.Hour * 2,
	}

	leaderAddr := net.JoinHostPort(broker.Host, strconv.Itoa(int(broker.Port)))
	conn, connErr := dialer.Dial("tcp", leaderAddr)
	if connErr != nil {
		logger.Println(connErr)
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

	offsetResponse := &OffsetResponse{}
	offsetResponse.Decode(responseBuf)

	return offsetResponse, nil
}
