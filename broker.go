package healer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/golang/glog"
)

type Broker struct {
	nodeID        int32
	address       string
	metaConn      net.Conn
	conn          net.Conn
	apiVersions   []*ApiVersion
	timeout       time.Duration // Second
	connecTimeout time.Duration // Second

	correlationID uint32
}

var defaultClientID = "healer"

// NewBroker is used just as bootstrap in NewBrokers.
// user must always init a Brokers instance by NewBrokers
func NewBroker(address string, nodeID int32, connecTimeout int, timeout int) (*Broker, error) {
	//TODO more parameters, timeout, keepalive, connect timeout ...
	//TODO get available api versions

	broker := &Broker{
		nodeID:  nodeID,
		address: address,
		timeout: time.Duration(timeout),

		correlationID: 0,
	}

	metaConn, err := net.DialTimeout("tcp", address, time.Duration(connecTimeout)*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to establish connection when init broker: %s", err)
	}
	broker.metaConn = metaConn

	conn, err := net.DialTimeout("tcp", address, time.Duration(connecTimeout)*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to establish connection when init broker: %s", err)
	}
	broker.conn = conn

	// TODO since ??
	//apiVersionsResponse, err := broker.requestApiVersions()
	//if err != nil {
	//return nil, fmt.Errorf("failed to request api versions when init broker: %s", err)
	//}
	//broker.apiVersions = apiVersionsResponse.ApiVersions

	return broker, nil
}

func (broker *Broker) Close() {
	broker.metaConn.Close()
	broker.conn.Close()
}

func (broker *Broker) request(payload []byte) ([]byte, error) {
	// TODO log?
	//glog.V(10).Infof("request length: %d", len(payload))
	broker.metaConn.Write(payload)

	l := 0
	responseLengthBuf := make([]byte, 4)
	for {
		if broker.timeout > 0 {
			broker.metaConn.SetReadDeadline(time.Now().Add(broker.timeout * time.Second))
		}
		length, err := broker.metaConn.Read(responseLengthBuf[l:])
		if err != nil {
			return nil, err
		}
		if length+l == 4 {
			break
		}
		l = length
	}

	responseLength := int(binary.BigEndian.Uint32(responseLengthBuf))
	//glog.V(10).Infof("response length: %d", responseLength)
	responseBuf := make([]byte, 4+responseLength)

	readLength := 0
	for {
		if broker.timeout > 0 {
			broker.metaConn.SetReadDeadline(time.Now().Add(broker.timeout * time.Second))
		}
		length, err := broker.metaConn.Read(responseBuf[4+readLength:])
		if err == io.EOF {
			break
		}

		if err != nil {
			glog.Errorf("read response error:%s", err)
			return nil, err
		}

		readLength += length
		if readLength > responseLength {
			return nil, errors.New("fetch more data than needed while read response")
		}
		if readLength == responseLength {
			break
		}
	}
	copy(responseBuf[0:4], responseLengthBuf)

	return responseBuf, nil
}

func (broker *Broker) requestStreamingly(payload []byte, buffers chan []byte) error {
	// TODO log?
	defer close(buffers)
	broker.conn.Write(payload)

	l := 0
	responseLengthBuf := make([]byte, 4)
	for {
		if broker.timeout > 0 {
			broker.conn.SetReadDeadline(time.Now().Add(broker.timeout * time.Second))
		}
		length, err := broker.conn.Read(responseLengthBuf[l:])

		glog.V(20).Infof("%v", responseLengthBuf[l:length])
		buffers <- responseLengthBuf[l:length]

		if err != nil {
			return err
		}
		if length+l == 4 {
			break
		}
		l = length
	}

	responseLength := int(binary.BigEndian.Uint32(responseLengthBuf))
	glog.V(10).Infof("response length: %d", responseLength)

	readLength := 0
	for {
		buf := make([]byte, 65535)
		length, err := broker.conn.Read(buf)
		glog.V(20).Infof("%v", buf[:length])
		glog.V(15).Infof("read %d bytes response", length)
		buffers <- buf[:length]
		//if err == io.EOF {
		//glog.V(10).Info("read EOF")
		//return errors.New("encounter EOF while read fetch response")
		//}

		if err != nil {
			glog.Errorf("read response error:%s", err)
			return err
		}
		readLength += length
		glog.V(10).Infof("totally send %d bytes to fetch response payload", readLength+4)
		if readLength > responseLength {
			return errors.New("fetch more data than needed while read fetch response")
		}
		if readLength == responseLength {
			glog.V(10).Info("read enough data, return")
			return nil
		}
	}
	return nil
}

func (broker *Broker) requestApiVersions(clientID string) (*ApiVersionsResponse, error) {
	// TODO should always use v0?
	broker.correlationID++
	apiVersionRequest := NewApiVersionsRequest(0, broker.correlationID, clientID)
	response, err := broker.request(apiVersionRequest.Encode())
	if err != nil {
		return nil, err
	}
	apiVersionsResponse, err := NewApiVersionsResponse(response)
	if err != nil {
		return nil, err
	}
	return apiVersionsResponse, nil
}

func (broker *Broker) requestListGroups(clientID string) (*ListGroupsResponse, error) {
	broker.correlationID++
	request := NewListGroupsRequest(broker.correlationID, clientID)

	payload := request.Encode()

	responseBuf, err := broker.request(payload)
	if err != nil {
		return nil, err
	}

	listGroupsResponse, err := NewListGroupsResponse(responseBuf)
	if err != nil {
		return nil, err
	}

	//TODO error info in the response
	return listGroupsResponse, nil
}

func (broker *Broker) requestMetaData(clientID string, topic *string) (*MetadataResponse, error) {
	broker.correlationID++
	metadataRequest := MetadataRequest{}
	metadataRequest.RequestHeader = &RequestHeader{
		ApiKey:        API_MetadataRequest,
		ApiVersion:    0,
		CorrelationID: broker.correlationID,
		ClientId:      clientID,
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

	metadataResponse, err := NewMetadataResponse(responseBuf)
	if err != nil {
		return nil, err
	}

	//TODO error info in the response
	return metadataResponse, nil
}

// RequestOffsets return the offset values array from ther broker. all partitionID in partitionIDs must be in THIS broker
func (broker *Broker) requestOffsets(clientID, topic string, partitionIDs []uint32, timeValue int64, offsets uint32) (*OffsetsResponse, error) {
	broker.correlationID++
	offsetsRequest := NewOffsetsRequest(topic, partitionIDs, timeValue, offsets, broker.correlationID, clientID)
	payload := offsetsRequest.Encode()

	responseBuf, err := broker.request(payload)
	if err != nil {
		return nil, err
	}

	offsetsResponse, err := NewOffsetsResponse(responseBuf)
	if err != nil {
		return nil, err
	}

	return offsetsResponse, nil
}

func (broker *Broker) requestFindCoordinator(clientID, groupID string) (*FindCoordinatorResponse, error) {
	broker.correlationID++
	findCoordinatorRequest := NewFindCoordinatorRequest(broker.correlationID, clientID, groupID)
	payload := findCoordinatorRequest.Encode()

	responseBuf, err := broker.request(payload)
	if err != nil {
		return nil, err
	}

	findCoordinatorResponse, err := NewFindCoordinatorResponse(responseBuf)
	if err != nil {
		return nil, err
	}

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

func (broker *Broker) requestFetchStreamingly(fetchRequest *FetchRequest, buffers chan []byte) error {
	payload := fetchRequest.Encode()

	// TODO 10?
	//go consumeFetchResponse(buffers, messages)
	err := broker.requestStreamingly(payload, buffers)
	glog.V(10).Info("requestFetchStreamingly return")
	return err
}

func (broker *Broker) findCoordinator(clientID, groupID string) (*FindCoordinatorResponse, error) {
	broker.correlationID++
	request := NewFindCoordinatorRequest(broker.correlationID, clientID, groupID)

	payload := request.Encode()

	responseBytes, err := broker.request(payload)
	if err != nil {
		return nil, err
	}
	return NewFindCoordinatorResponse(responseBytes)
}

func (broker *Broker) requestJoinGroup(clientID, groupID string, sessionTimeout int32, memberID, protocolType string, gps []*GroupProtocol) (*JoinGroupResponse, error) {
	broker.correlationID++
	joinGroupRequest := NewJoinGroupRequest(
		broker.correlationID, clientID, groupID, sessionTimeout, memberID, protocolType)
	for _, gp := range gps {
		joinGroupRequest.AddGroupProtocal(gp)
	}

	payload := joinGroupRequest.Encode()

	responseBytes, err := broker.request(payload)
	if err != nil {
		return nil, err
	}

	return NewJoinGroupResponse(responseBytes)
}

func (broker *Broker) requestDescribeGroups(clientID string, groups []string) (*DescribeGroupsResponse, error) {
	broker.correlationID++
	joinGroupRequest := NewDescribeGroupsRequest(broker.correlationID, clientID, groups)

	payload := joinGroupRequest.Encode()

	responseBytes, err := broker.request(payload)
	if err != nil {
		return nil, err
	}

	return NewDescribeGroupsResponse(responseBytes)
}

func (broker *Broker) requestSyncGroup(clientID, groupID string, generationID int32, memberID string, groupAssignment GroupAssignment) (*SyncGroupResponse, error) {
	broker.correlationID++
	syncGroupRequest := NewSyncGroupRequest(broker.correlationID, clientID, groupID, generationID, memberID, groupAssignment)
	payload := syncGroupRequest.Encode()

	responseBytes, err := broker.request(payload)
	if err != nil {
		return nil, err
	}

	return NewSyncGroupResponse(responseBytes)
}

func (broker *Broker) requestHeartbeat(clientID, groupID string, generationID int32, memberID string) (*HeartbeatResponse, error) {
	broker.correlationID++
	r := NewHeartbeatRequest(broker.correlationID, clientID, groupID, generationID, memberID)

	responseBytes, err := broker.request(r.Encode())
	if err != nil {
		return nil, err
	}

	return NewHeartbeatResponse(responseBytes)
}
