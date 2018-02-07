package healer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/golang/glog"
)

type Broker struct {
	nodeID        int32
	address       string
	conn          net.Conn
	apiVersions   []*ApiVersion
	timeout       time.Duration // Second
	connecTimeout time.Duration // Second

	//since each client should have one broker, so maybe broker should has the same clientID with client?
	//clientID string

	correlationID uint32

	mux sync.Mutex

	dead bool
}

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
		dead:          true,
	}

	conn, err := net.DialTimeout("tcp4", address, time.Duration(connecTimeout)*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to establish connection when init broker: %s", err)
	}
	broker.conn = conn
	broker.dead = false

	// TODO since ??
	//apiVersionsResponse, err := broker.requestApiVersions()
	//if err != nil {
	//return nil, fmt.Errorf("failed to request api versions when init broker: %s", err)
	//}
	//broker.apiVersions = apiVersionsResponse.ApiVersions

	return broker, nil
}

func (broker *Broker) GetAddress() string {
	return broker.address
}

func (broker *Broker) Close() {
	broker.conn.Close()
}

func (broker *Broker) IsDead() bool {
	return broker.dead
}

func (broker *Broker) Request(r Request) ([]byte, error) {
	broker.correlationID++
	r.SetCorrelationID(broker.correlationID)
	return broker.request(r.Encode())
}

func (broker *Broker) request(payload []byte) ([]byte, error) {
	broker.mux.Lock()
	defer broker.mux.Unlock()
	// TODO log?
	glog.V(10).Info(broker.conn.LocalAddr())
	glog.V(10).Infof("request length: %d. api: %d CorrelationID: %d", len(payload), binary.BigEndian.Uint16(payload[4:]), binary.BigEndian.Uint32(payload[8:]))
	n, err := broker.conn.Write(payload)
	if err != nil {
		glog.Error(err)
	}
	if n != len(payload) {
		glog.Errorf("write only partial data. api: %d CorrelationID: %d", binary.BigEndian.Uint16(payload[4:]), binary.BigEndian.Uint32(payload[8:]))
	}

	l := 0
	responseLengthBuf := make([]byte, 4)
	for {
		if broker.timeout > 0 {
			broker.conn.SetReadDeadline(time.Now().Add(broker.timeout * time.Second))
		}
		length, err := broker.conn.Read(responseLengthBuf[l:])
		if err != nil {
			if err == io.EOF {
				broker.dead = true
			}
			return nil, err
		}

		if length+l == 4 {
			break
		}
		l = length
	}

	responseLength := int(binary.BigEndian.Uint32(responseLengthBuf))
	glog.V(10).Infof("response length: %d", responseLength+4)
	responseBuf := make([]byte, 4+responseLength)

	readLength := 0
	for {
		if broker.timeout > 0 {
			broker.conn.SetReadDeadline(time.Now().Add(broker.timeout * time.Second))
		}
		length, err := broker.conn.Read(responseBuf[4+readLength:])
		if err != nil {
			if err == io.EOF {
				broker.dead = true
			}
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
	glog.V(10).Infof("response length: %d. CorrelationID: %d", len(responseBuf), binary.BigEndian.Uint32(responseBuf[4:]))
	glog.V(100).Infof("response:%v", responseBuf)

	return responseBuf, nil
}

func (broker *Broker) requestStreamingly(payload []byte, buffers chan []byte) error {
	defer close(buffers)

	glog.V(10).Infof("request length: %d. api: %d CorrelationID: %d", len(payload), binary.BigEndian.Uint16(payload[4:]), binary.BigEndian.Uint32(payload[8:]))
	n, err := broker.conn.Write(payload)
	if err != nil {
		glog.Error(err)
	}
	if n != len(payload) {
		glog.Errorf("write only partial data. api: %d CorrelationID: %d", binary.BigEndian.Uint16(payload[4:]), binary.BigEndian.Uint32(payload[8:]))
	}

	l := 0
	responseLengthBuf := make([]byte, 4)
	for {
		if broker.timeout > 0 {
			broker.conn.SetReadDeadline(time.Now().Add(broker.timeout * time.Second))
		}
		length, err := broker.conn.Read(responseLengthBuf[l:])
		if err != nil {
			if err == io.EOF {
				broker.dead = true
			}
			return err
		}

		glog.V(20).Infof("%v", responseLengthBuf[l:length])
		buffers <- responseLengthBuf[l:length]

		if length+l == 4 {
			break
		}
		l += length
	}

	responseLength := int(binary.BigEndian.Uint32(responseLengthBuf))
	glog.V(10).Infof("total response length should be %d", 4+responseLength)

	readLength := 0
	for {
		buf := make([]byte, 65535)
		if broker.timeout > 0 {
			broker.conn.SetReadDeadline(time.Now().Add(broker.timeout * time.Second))
		}
		length, err := broker.conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				broker.dead = true
			}
			return err
		}

		glog.V(100).Infof("%v", buf[:length])
		glog.V(15).Infof("read %d bytes response", length)
		buffers <- buf[:length]

		readLength += length
		glog.V(12).Infof("totally send %d/%d bytes to fetch response payload", readLength+4, responseLength+4)
		if readLength > responseLength {
			return errors.New("fetch more data than needed while read fetch response")
		}
		if readLength == responseLength {
			glog.V(15).Info("read enough data, return")
			return nil
		}
	}
	return nil
}

func (broker *Broker) requestApiVersions(clientID string) (*ApiVersionsResponse, error) {
	// TODO should always use v0?
	apiVersionRequest := NewApiVersionsRequest(0, clientID)
	response, err := broker.Request(apiVersionRequest)
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
	request := NewListGroupsRequest(clientID)

	responseBuf, err := broker.Request(request)
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
	metadataRequest := &MetadataRequest{}
	metadataRequest.RequestHeader = &RequestHeader{
		ApiKey:     API_MetadataRequest,
		ApiVersion: 0,
		ClientId:   clientID,
	}

	if topic != nil {
		metadataRequest.Topic = []string{*topic}
	} else {
		metadataRequest.Topic = []string{}
	}

	responseBuf, err := broker.Request(metadataRequest)
	if err != nil {
		return nil, err
	}

	return NewMetadataResponse(responseBuf)
}

// RequestOffsets return the offset values array from ther broker. all partitionID in partitionIDs must be in THIS broker
func (broker *Broker) requestOffsets(clientID, topic string, partitionIDs []uint32, timeValue int64, offsets uint32) (*OffsetsResponse, error) {
	offsetsRequest := NewOffsetsRequest(topic, partitionIDs, timeValue, offsets, clientID)

	responseBuf, err := broker.Request(offsetsRequest)
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
	findCoordinatorRequest := NewFindCoordinatorRequest(clientID, groupID)

	responseBuf, err := broker.Request(findCoordinatorRequest)
	if err != nil {
		return nil, err
	}

	findCoordinatorResponse, err := NewFindCoordinatorResponse(responseBuf)
	if err != nil {
		return nil, err
	}

	return findCoordinatorResponse, nil
}

func (broker *Broker) requestFetchStreamingly(fetchRequest *FetchRequest, buffers chan []byte) error {
	broker.mux.Lock()
	defer broker.mux.Unlock()

	broker.correlationID++
	fetchRequest.SetCorrelationID(broker.correlationID)
	payload := fetchRequest.Encode()

	return broker.requestStreamingly(payload, buffers)
}

func (broker *Broker) findCoordinator(clientID, groupID string) (*FindCoordinatorResponse, error) {
	request := NewFindCoordinatorRequest(clientID, groupID)

	responseBytes, err := broker.Request(request)
	if err != nil {
		return nil, err
	}
	return NewFindCoordinatorResponse(responseBytes)
}

func (broker *Broker) requestJoinGroup(clientID, groupID string, sessionTimeout int32, memberID, protocolType string, gps []*GroupProtocol) (*JoinGroupResponse, error) {
	joinGroupRequest := NewJoinGroupRequest(clientID, groupID, sessionTimeout, memberID, protocolType)
	for _, gp := range gps {
		joinGroupRequest.AddGroupProtocal(gp)
	}

	responseBytes, err := broker.Request(joinGroupRequest)
	if err != nil {
		return nil, err
	}

	return NewJoinGroupResponse(responseBytes)
}

func (broker *Broker) requestDescribeGroups(clientID string, groups []string) (*DescribeGroupsResponse, error) {
	req := NewDescribeGroupsRequest(clientID, groups)

	responseBytes, err := broker.Request(req)
	if err != nil {
		return nil, err
	}

	return NewDescribeGroupsResponse(responseBytes)
}

func (broker *Broker) requestSyncGroup(clientID, groupID string, generationID int32, memberID string, groupAssignment GroupAssignment) (*SyncGroupResponse, error) {
	syncGroupRequest := NewSyncGroupRequest(clientID, groupID, generationID, memberID, groupAssignment)

	responseBytes, err := broker.Request(syncGroupRequest)
	if err != nil {
		return nil, err
	}

	return NewSyncGroupResponse(responseBytes)
}

func (broker *Broker) requestHeartbeat(clientID, groupID string, generationID int32, memberID string) (*HeartbeatResponse, error) {
	r := NewHeartbeatRequest(clientID, groupID, generationID, memberID)

	responseBytes, err := broker.Request(r)
	if err != nil {
		return nil, err
	}

	return NewHeartbeatResponse(responseBytes)
}
