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
	nodeID      int32
	address     string
	clientID    string
	conn        net.Conn
	apiVersions []*ApiVersion
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
		return nil, fmt.Errorf("failed to establish connection when init broker: %s", err)
	}
	broker.conn = conn

	apiVersionsResponse, err := broker.requestApiVersions()
	if err != nil {
		return nil, fmt.Errorf("failed to request api versions when init broker: %s", err)
	}
	broker.apiVersions = apiVersionsResponse.ApiVersions

	return broker, nil
}

func (broker *Broker) Close() error {
	return broker.conn.Close()
}

func (broker *Broker) request(payload []byte) ([]byte, error) {
	// TODO log?
	//glog.V(10).Infof("request length: %d", len(payload))
	broker.conn.Write(payload)

	responseLengthBuf := make([]byte, 4)
	_, err := broker.conn.Read(responseLengthBuf)
	if err != nil {
		return nil, err
	}

	responseLength := int(binary.BigEndian.Uint32(responseLengthBuf))
	//glog.V(10).Infof("response length: %d", responseLength)
	responseBuf := make([]byte, 4+responseLength)

	readLength := 0
	for {
		length, err := broker.conn.Read(responseBuf[4+readLength:])
		if err == io.EOF {
			break
		}

		if err != nil {
			glog.Errorf("read response error:%s", err)
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

func (broker *Broker) requestApiVersions() (*ApiVersionsResponse, error) {
	correlationID := int32(os.Getpid())
	apiVersionRequest := NewApiVersionsRequest(0, correlationID, broker.clientID)
	response, err := broker.request(apiVersionRequest.Encode())
	if err != nil {
		return nil, err
	}
	apiVersionsResponse := &ApiVersionsResponse{}
	err = apiVersionsResponse.Decode(response)
	if err != nil {
		return nil, err
	}
	return apiVersionsResponse, nil
}

func (broker *Broker) requestStreamingly(payload []byte, buffers chan []byte) error {
	// TODO log?
	broker.conn.Write(payload)

	buf := make([]byte, 65535)
	for {
		length, err := broker.conn.Read(buf)
		buffers <- buf[:length]
		if err == io.EOF {
			return nil
		}

		if err != nil {
			glog.Errorf("read response error:%s", err)
			return err
		}
	}

	return nil
}

func (broker *Broker) requestMetaData(topic *string) (*MetadataResponse, error) {
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

// RequestOffsets return the offset values array from ther broker. all partitionID in partitionIDs must be in THIS broker
func (broker *Broker) requestOffsets(topic string, partitionIDs []uint32, timeValue int64, offsets uint32) (*OffsetsResponse, error) {
	correlationID := int32(os.Getpid())

	offsetsRequest := NewOffsetsRequest(topic, partitionIDs, timeValue, offsets, correlationID, broker.clientID)
	payload := offsetsRequest.Encode()

	responseBuf, err := broker.request(payload)
	if err != nil {
		return nil, err
	}

	offsetsResponse := &OffsetsResponse{}
	offsetsResponse.Decode(responseBuf)

	return offsetsResponse, nil
}

func (broker *Broker) requestFindCoordinator(groupID string) (*FindCoordinatorResponse, error) {
	correlationID := int32(os.Getpid())

	findCoordinatorRequest := NewFindCoordinatorRequest(correlationID, broker.clientID, groupID)
	payload := findCoordinatorRequest.Encode()

	responseBuf, err := broker.request(payload)
	if err != nil {
		return nil, err
	}

	findCoordinatorResponse := &FindCoordinatorResponse{}
	findCoordinatorResponse.Decode(responseBuf)

	return findCoordinatorResponse, nil
}

// TODO should assemble MessageSets streamingly
func (broker *Broker) requestFetch(fetchRequest *FetchRequest) (*FetchResponse, error) {
	payload := fetchRequest.Encode()

	responseBuf, err := broker.request(payload)
	if err != nil {
		return nil, err
	}

	fetchResponse := &FetchResponse{}
	fetchResponse.Decode(responseBuf)
	return fetchResponse, nil
}

func (broker *Broker) requestFetchStreamingly(fetchRequest *FetchRequest, messages chan *Message) error {
	payload := fetchRequest.Encode()

	// TODO 10?
	buffers := make(chan []byte, 10)
	err := broker.requestStreamingly(payload, buffers)
	if err != nil {
		return err
	}
	consumeFetchResponse(buffers, messages)
	return nil
}
