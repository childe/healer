package healer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/golang/glog"
)

type Broker struct {
	config  *BrokerConfig
	address string
	nodeID  int32

	conn        net.Conn
	apiVersions []*ApiVersion

	//since each client should have one broker, so maybe broker should has the same clientID with client?
	// TODO different clientID should have independent broker?
	//clientID string

	correlationID uint32

	mux sync.Mutex

	dead bool
}

// NewBroker is used just as bootstrap in NewBrokers.
// user must always init a Brokers instance by NewBrokers
func NewBroker(address string, nodeID int32, config *BrokerConfig) (*Broker, error) {
	//TODO get available api versions
	broker := &Broker{
		config:  config,
		address: address,
		nodeID:  nodeID,

		correlationID: 0,
		dead:          true,
	}

	conn, err := net.DialTimeout("tcp4", address, time.Duration(config.ConnectTimeoutMS)*time.Millisecond)
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

func (broker *Broker) ensureOpen() {
	if broker.dead {
		glog.Infof("broker %s dead, reopen it", broker.address)
		conn, err := net.DialTimeout("tcp4", broker.address, time.Duration(broker.config.ConnectTimeoutMS)*time.Millisecond)
		if err != nil {
			// TODO fatal?
			glog.Fatalf("could not conn to %s:%s", broker.address, err)
		}
		broker.conn = conn
		broker.dead = false
	}
}

func (broker *Broker) Request(r Request) ([]byte, error) {
	broker.mux.Lock()
	defer broker.mux.Unlock()

	broker.ensureOpen()

	broker.correlationID++
	r.SetCorrelationID(broker.correlationID)
	timeout := broker.config.TimeoutMS
	if len(broker.config.TimeoutMSForEachAPI) > int(r.API()) {
		timeout = broker.config.TimeoutMSForEachAPI[r.API()]
	}
	return broker.request(r.Encode(), timeout)
}

func (broker *Broker) request(payload []byte, timeout int) ([]byte, error) {
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
		if timeout > 0 {
			broker.conn.SetReadDeadline(time.Now().Add(time.Duration(timeout) * time.Millisecond))
		}
		length, err := broker.conn.Read(responseLengthBuf[l:])
		if err != nil {
			broker.dead = true
			broker.Close()
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
		if timeout > 0 {
			broker.conn.SetReadDeadline(time.Now().Add(time.Duration(timeout) * time.Millisecond))
		}
		length, err := broker.conn.Read(responseBuf[4+readLength:])
		if err != nil {
			broker.dead = true
			broker.Close()
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

func (broker *Broker) requestStreamingly(payload []byte, buffers chan []byte, timeout int) error {
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
		if timeout > 0 {
			broker.conn.SetReadDeadline(time.Now().Add(time.Duration(timeout) * time.Millisecond))
		}
		length, err := broker.conn.Read(responseLengthBuf[l:])
		if err != nil {
			broker.dead = true
			broker.Close()
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
		if timeout > 0 {
			broker.conn.SetReadDeadline(time.Now().Add(time.Duration(timeout) * time.Millisecond))
		}
		length, err := broker.conn.Read(buf)
		if err != nil {
			broker.dead = true
			broker.Close()
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

func (broker *Broker) requestMetaData(clientID string, topics []string) (*MetadataResponse, error) {
	metadataRequest := &MetadataRequest{
		Topics: topics,
	}
	metadataRequest.RequestHeader = &RequestHeader{
		ApiKey:     API_MetadataRequest,
		ApiVersion: 0,
		ClientId:   clientID,
	}

	responseBuf, err := broker.Request(metadataRequest)
	if err != nil {
		return nil, err
	}

	return NewMetadataResponse(responseBuf)
}

// RequestOffsets return the offset values array from ther broker. all partitionID in partitionIDs must be in THIS broker
func (broker *Broker) requestOffsets(clientID, topic string, partitionIDs []int32, timeValue int64, offsets uint32) (*OffsetsResponse, error) {
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

	broker.ensureOpen()

	broker.correlationID++
	fetchRequest.SetCorrelationID(broker.correlationID)
	payload := fetchRequest.Encode()

	timeout := broker.config.TimeoutMS
	if len(broker.config.TimeoutMSForEachAPI) > int(fetchRequest.API()) {
		timeout = broker.config.TimeoutMSForEachAPI[fetchRequest.API()]
	}

	return broker.requestStreamingly(payload, buffers, timeout)
}

func (broker *Broker) findCoordinator(clientID, groupID string) (*FindCoordinatorResponse, error) {
	request := NewFindCoordinatorRequest(clientID, groupID)

	responseBytes, err := broker.Request(request)
	if err != nil {
		return nil, err
	}
	return NewFindCoordinatorResponse(responseBytes)
}

func (broker *Broker) requestJoinGroup(clientID, groupID string, sessionTimeoutMS int32, memberID, protocolType string, gps []*GroupProtocol) (*JoinGroupResponse, error) {
	joinGroupRequest := NewJoinGroupRequest(clientID, groupID, sessionTimeoutMS, memberID, protocolType)
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
