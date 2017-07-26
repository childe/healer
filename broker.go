package healer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/golang/glog"
)

type Broker struct {
	nodeID   int32
	address  string
	clientID string
	conn     net.Conn
}

var defaultClientID = "healer"

// NewBroker is used just as bootstrap in NewBrokers.
// user must always init a Brokers instance by NewBrokers
func NewBroker(address string, clientID string, nodeID int32) (*Broker, error) {
	//TODO more parameters, timeout, keepalive, connect timeout ...
	//TODO get available api versions
	if clientID == "" {
		clientID = defaultClientID
	}

	broker := &Broker{
		nodeID:   nodeID,
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

func (broker *Broker) request(payload []byte) ([]byte, error) {
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

	return responseBuf, nil
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

	responseBuf, err := broker.request(payload)
	if err != nil {
		return nil, err
	}

	metadataResponse := &MetadataResponse{}
	err = metadataResponse.Decode(responseBuf)
	if err != nil {
		return nil, err
	}

	//TODO error info in the response
	return metadataResponse, nil
}

// GetOffset return the offset values array from server
func (broker *Broker) RequestOffsets(topic string, partitionID int32, timeValue int64, offsets uint32) ([]*OffsetsResponse, error) {
	if partitionID < 0 {
		rst := make([]*OffsetsResponse, 0)
		metadataResponse, err := broker.RequestMetaData(&topic)
		if err != nil {
			return nil, fmt.Errorf("could not get metadata of topic[%s]:%s", topic, err)
		}

		// TODO only one topic
		topicMetadata := metadataResponse.TopicMetadatas[0]

		if topicMetadata.TopicErrorCode != 0 {
			return nil, AllError[topicMetadata.TopicErrorCode]
		}

		for _, x := range topicMetadata.PartitionMetadatas {
			offsetsResponseList, err := broker.RequestOffsets(topic, x.PartitionId, timeValue, offsets)
			if err != nil {
				return nil, err
			}
			rst = append(rst, offsetsResponseList...)
		}
		return rst, nil
	}
	correlationID := int32(os.Getpid())

	offsetsRequest := NewOffsetsRequest(topic, []uint32{uint32(partitionID)}, timeValue, offsets, correlationID, broker.clientID)
	payload := offsetsRequest.Encode()
	glog.V(10).Infof("offset request payload is prepared")

	responseBuf, err := broker.request(payload)
	if err != nil {
		return nil, err
	}

	offsetsResponse := &OffsetsResponse{}
	offsetsResponse.Decode(responseBuf)

	return []*OffsetsResponse{offsetsResponse}, nil
}
