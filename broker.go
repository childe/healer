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

func NewBroker(address string) (*Broker, error) {
	broker := &Broker{}
	conn, err := net.DialTimeout("tcp", address, time.Second*5)
	if err != nil {
		return nil, err
	}
	broker.conn = conn
	return broker, nil
}

func (broker *Broker) RequestMetaData(topic string) (*MetadataResponse, error) {
	correlationID := int32(os.Getpid())
	metadataRequest := MetadataRequest{}
	metadataRequest.RequestHeader = &RequestHeader{
		ApiKey:        API_MetadataRequest,
		ApiVersion:    0,
		CorrelationId: correlationID,
		ClientId:      broker.clientID,
	}
	metadataRequest.Topic = []string{topic}

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
	return metadataResponse, nil
}
