package gokafka

import (
	"net"
	"time"
)

func GetMetaData(broker string, topic string, correlationId int32, clientId string) (*MetadataResponse, error) {
	metadataRequest := MetadataRequest{}
	metadataRequest.RequestHeader = &RequestHeader{
		ApiKey:        API_MetadataRequest,
		ApiVersion:    0,
		CorrelationId: 0,
		ClientId:      clientId,
	}
	metadataRequest.Topic = []string{topic}

	conn, err := net.DialTimeout("tcp", broker, time.Second*5)
	if err != nil {
		return nil, err
	}
	payload := metadataRequest.Encode()
	conn.Write(payload)

	responsePlayload := make([]byte, 10240)
	length, _ := conn.Read(responsePlayload)
	conn.Close()

	metadataResponse := &MetadataResponse{}
	err = metadataResponse.Decode(responsePlayload[:length])
	if err != nil {
		return nil, err
	}
	return metadataResponse, nil
}
