package healer

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"time"
)

type Broker struct {
	address  string
	clientID string
	conn     net.Conn
}

var defaultClientID = "healer"

func NewBroker(address string, clientID string) (*Broker, error) {
	//TODO more parameters, timeout, keepalive, connect timeout ...
	if clientID == "" {
		clientID = defaultClientID
	}

	broker := &Broker{
		address:  address,
		clientID: clientID,
	}

	conn, err := net.DialTimeout("tcp", address, time.Second*5)
	if err != nil {
		return nil, err
	}
	broker.conn = conn

	return broker, nil
}

func (broker *Broker) RequestMetaData(topic *string) (*MetadataResponse, error) {
	correlationID := int32(os.Getpid())
	metadataRequest := MetadataRequest{}
	metadataRequest.RequestHeader = &RequestHeader{
		ApiKey:        API_MetadataRequest,
		ApiVersion:    0,
		CorrelationId: correlationID,
		ClientId:      broker.clientID,
	}

	if topic != nil {
		metadataRequest.Topic = []string{*topic}
	} else {
		metadataRequest.Topic = []string{}
	}

	payload := metadataRequest.Encode()
	broker.conn.Write(payload)

	responseLengthBuf := make([]byte, 4)
	_, err := broker.conn.Read(responseLengthBuf)
	if err != nil {
		return nil, err
	}

	responseLength := int(binary.BigEndian.Uint32(responseLengthBuf))
	responseBuf := make([]byte, 4+responseLength)

	readLength := 0
	for {
		length, err := broker.conn.Read(responseBuf[4+readLength:])
		if err == io.EOF {
			break
		}

		if err != nil {
			logger.Fatalln(err)
			return nil, err
		}

		readLength += length
		if readLength > responseLength {
			return nil, errors.New("fetch more data than needed while read getMetaData response")
		}
		if readLength == responseLength {
			break
		}
	}
	copy(responseBuf[0:4], responseLengthBuf)

	metadataResponse := &MetadataResponse{}
	err = metadataResponse.Decode(responseBuf)
	if err != nil {
		return nil, err
	}

	//TODO error info in the response
	return metadataResponse, nil
}

// GetOffset return the offset values array from server
func (broker *Broker) RequestOffsets(topic *string, partitionID int32, timeValue int64, offsets uint32) (*OffsetResponse, error) {
	correlationID := int32(os.Getpid())

	requestHeader := &RequestHeader{
		ApiKey:        API_OffsetRequest,
		ApiVersion:    0,
		CorrelationId: correlationID,
		ClientId:      broker.clientID,
	}

	partitionOffsetRequestInfos := make(map[uint32]*PartitionOffsetRequestInfo)
	partitionOffsetRequestInfos[uint32(partitionID)] = &PartitionOffsetRequestInfo{
		Time:               timeValue,
		MaxNumberOfOffsets: offsets,
	}
	topicOffsetRequestInfos := make(map[string]map[uint32]*PartitionOffsetRequestInfo)
	topicOffsetRequestInfos[*topic] = partitionOffsetRequestInfos

	offsetReqeust := &OffsetReqeust{
		RequestHeader: requestHeader,
		ReplicaId:     -1,
		RequestInfo:   topicOffsetRequestInfos,
	}

	payload := offsetReqeust.Encode()

	broker.conn.Write(payload)

	responseLengthBuf := make([]byte, 4)
	_, err := broker.conn.Read(responseLengthBuf)
	if err != nil {
		return nil, err
	}

	responseLength := int(binary.BigEndian.Uint32(responseLengthBuf))
	responseBuf := make([]byte, 4+responseLength)

	readLength := 0
	for {
		length, err := broker.conn.Read(responseBuf[4+readLength:])
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		readLength += length
		if readLength > responseLength {
			return nil, errors.New("fetch more data than needed while read getMetaData response")
		}
	}

	if readLength != responseLength {
		return nil, errors.New("do NOT fetch needed length while read getMetaData response")
	}
	copy(responseBuf[0:4], responseLengthBuf)

	offsetResponse := &OffsetResponse{}
	offsetResponse.Decode(responseBuf)

	return offsetResponse, nil
}
