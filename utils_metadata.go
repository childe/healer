package healer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

// GetMetaData return one MetadataResponse object
func GetMetaData(brokerList string, topic string, correlationID uint32, clientID string) (*MetadataResponse, error) {
	for _, broker := range strings.Split(brokerList, ",") {
		fmt.Println(broker)
		metadataRequest := MetadataRequest{}
		metadataRequest.RequestHeader = &RequestHeader{
			ApiKey:        API_MetadataRequest,
			ApiVersion:    0,
			CorrelationID: correlationID,
			ClientId:      clientID,
		}
		metadataRequest.Topic = []string{topic}

		conn, err := net.DialTimeout("tcp", broker, time.Second*5)
		if err != nil {
			return nil, err
		}
		payload := metadataRequest.Encode()
		conn.Write(payload)

		responseLengthBuf := make([]byte, 4)
		_, err = conn.Read(responseLengthBuf)
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
				//logger.Fatalln(err)
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

		metadataResponse, err := NewMetadataResponse(responseBuf)
		if err != nil {
			return nil, err
		}
		conn.Close()
		return metadataResponse, nil
	}

	return nil, errors.New("could not get metadata from all brokers")
}
