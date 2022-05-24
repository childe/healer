package healer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
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

var (
	errTLSConfig = errors.New("Cert & File & CA must be set")
)

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

	conn, err := newConn(address, config)
	if err != nil {
		return nil, fmt.Errorf("failed to establish connection when init broker: %s", err)
	}
	broker.conn = conn
	broker.dead = false

	if broker.config.SaslConfig != nil {
		if err := broker.sendSaslAuthenticate(); err != nil {
			glog.Errorf("sasl authenticate error : %s", err)
			return nil, err
		}
	}

	if config.KafkaVersion == "" || compareKafkaVersion(config.KafkaVersion, "0.10.0.0") >= 0 {
		clientID := "healer-init"
		apiVersionsResponse, err := broker.requestApiVersions(clientID)
		if err != nil {
			return nil, fmt.Errorf("failed to request api versions when init broker: %s", err)
		}
		broker.apiVersions = apiVersionsResponse.ApiVersions
		if err != nil {
			return nil, fmt.Errorf("failed to marshal api versions when init broker: %s", err)
		}

		if glog.V(10) {
			glog.Infof("broker %s api versions:", address)
			var versions string
			for i, v := range broker.apiVersions {
				if i+1 == len(broker.apiVersions) {
					versions += fmt.Sprintf("%s:%d-%d", v.apiKey.String(), v.minVersion, v.maxVersion)
				} else {
					versions += fmt.Sprintf("%s:%d-%d,", v.apiKey.String(), v.minVersion, v.maxVersion)
				}
			}
			glog.Infof("broker %s api versions: %s", address, versions)
		}
	}

	return broker, nil
}

func (broker *Broker) getHighestAvailableAPIVersion(apiKey uint16) uint16 {
	versions, ok := availableVersions[apiKey]

	if !ok {
		return 0
	}

	for _, version := range versions {
		for _, brokerAPIVersion := range broker.apiVersions {
			if uint16(brokerAPIVersion.apiKey) == apiKey && brokerAPIVersion.minVersion <= version && brokerAPIVersion.maxVersion >= version {
				return version
			}
		}
	}
	return 0
}

func newConn(address string, config *BrokerConfig) (net.Conn, error) {
	dialer := net.Dialer{
		Timeout:   time.Millisecond * time.Duration(config.ConnectTimeoutMS),
		KeepAlive: time.Millisecond * time.Duration(config.KeepAliveMS),
	}

	if config.TLSEnabled {
		if config.TLS == nil || config.TLS.Cert == "" || config.TLS.Key == "" || config.TLS.CA == "" {
			return nil, errTLSConfig
		}
		if tlsConfig, err := createTLSConfig(config.TLS); err != nil {
			return nil, err
		} else {
			return tls.DialWithDialer(&dialer, "tcp", address, tlsConfig)
		}
	} else {
		return dialer.Dial("tcp", address)
	}
}

func createTLSConfig(tlsConfig *TLSConfig) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(tlsConfig.Cert, tlsConfig.Key)
	if err != nil {
		return nil, err
	}

	caCert, err := ioutil.ReadFile(tlsConfig.CA)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	t := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: tlsConfig.InsecureSkipVerify,
	}

	if tlsConfig.ServerName != "" {
		t.ServerName = tlsConfig.ServerName
	}

	return t, nil
}

func (broker *Broker) sendSaslAuthenticate() error {
	var clientID = ""
	var (
		saslConfig = broker.config.SaslConfig
		mechanism  = saslConfig.SaslMechanism
		user       = saslConfig.SaslUser
		password   = saslConfig.SaslPassword

		payload []byte
		err     error
	)
	saslHandShakeRequest := NewSaslHandShakeRequest(clientID, mechanism)

	payload = saslHandShakeRequest.Encode()

	timeout := broker.config.TimeoutMS
	payload, err = broker.request(saslHandShakeRequest.Encode(), timeout)
	if err != nil {
		return err
	}

	_, err = NewSaslHandshakeResponse(payload)
	if err != nil {
		return err
	}

	// authenticate
	saslAuthenticateRequest := NewSaslAuthenticateRequest(clientID, user, password, mechanism)
	payload, err = broker.request(saslAuthenticateRequest.Encode(), timeout)
	if err != nil {
		return err
	}
	_, err = NewSaslAuthenticateResponse(payload)
	if err != nil {
		return err
	}
	return nil
}

func (broker *Broker) GetAddress() string {
	return broker.address
}

func (broker *Broker) Close() {
	broker.dead = true
	broker.conn.Close()
}

func (broker *Broker) IsDead() bool {
	return broker.dead
}

func (broker *Broker) ensureOpen() error {
	if broker.dead {
		glog.Infof("broker %s is dead, (re)open it after sleep 200ms", broker.address)
		time.Sleep(time.Millisecond * 200)
		conn, err := newConn(broker.address, broker.config)
		if err != nil {
			glog.Errorf("could not conn to %s: %s", broker.address, err)
			return err
		}

		broker.conn = conn
		broker.dead = false
		broker.correlationID = 0

		if broker.config.SaslConfig != nil {
			if err := broker.sendSaslAuthenticate(); err != nil {
				glog.Errorf("sasl authenticate error : %s", err)
				return err
			}
		}
	}
	return nil
}

func (broker *Broker) Request(r Request) ([]byte, error) {
	broker.mux.Lock()
	defer broker.mux.Unlock()

	if err := broker.ensureOpen(); err != nil {
		return nil, err
	}

	broker.correlationID++
	r.SetCorrelationID(broker.correlationID)
	timeout := broker.config.TimeoutMS
	if len(broker.config.TimeoutMSForEachAPI) > int(r.API()) {
		timeout = broker.config.TimeoutMSForEachAPI[r.API()]
	}
	return broker.request(r.Encode(), timeout)
}

func (broker *Broker) request(payload []byte, timeout int) ([]byte, error) {
	if glog.V(10) {
		glog.Infof("%s -> %s", broker.conn.LocalAddr(), broker.conn.RemoteAddr())
		glog.Infof("request length: %d. api: %d CorrelationID: %d timeout: %d", len(payload), binary.BigEndian.Uint16(payload[4:]), binary.BigEndian.Uint32(payload[8:]), timeout)
	}
	for len(payload) > 0 {
		n, err := broker.conn.Write(payload)
		if err != nil {
			broker.Close()
			glog.Error(err)
			return nil, err
		}
		payload = payload[n:]
	}

	l := 0
	responseLengthBuf := make([]byte, 4)
	for {
		if timeout > 0 {
			broker.conn.SetReadDeadline(time.Now().Add(time.Duration(timeout) * time.Millisecond))
		}
		length, err := broker.conn.Read(responseLengthBuf[l:])
		if err != nil {
			broker.Close()
			return nil, err
		}

		if length+l == 4 {
			break
		}
		l += length
	}

	responseLength := int(binary.BigEndian.Uint32(responseLengthBuf))
	glog.V(10).Infof("response length in header: %d", responseLength+4)
	responseBuf := make([]byte, 4+responseLength)

	readLength := 0
	for {
		if timeout > 0 {
			broker.conn.SetReadDeadline(time.Now().Add(time.Duration(timeout) * time.Millisecond))
		}
		length, err := broker.conn.Read(responseBuf[4+readLength:])
		if err != nil {
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

func (broker *Broker) requestStreamingly(ctx context.Context, payload []byte, buffers chan []byte, timeout int) error {
	if glog.V(10) {
		glog.Infof("request length: %d. api: %d CorrelationID: %d", len(payload), binary.BigEndian.Uint16(payload[4:]), binary.BigEndian.Uint32(payload[8:]))
	}

	for len(payload) > 0 {
		n, err := broker.conn.Write(payload)
		if err != nil {
			broker.Close()
			return err
		}
		payload = payload[n:]
	}

	l := 0
	responseLengthBuf := make([]byte, 4)
	for {
		if timeout > 0 {
			broker.conn.SetReadDeadline(time.Now().Add(time.Duration(timeout) * time.Millisecond))
		}
		length, err := broker.conn.Read(responseLengthBuf[l:])
		if err != nil {
			broker.Close()
			return err
		}

		select {
		case <-ctx.Done():
			glog.Info("stop fetching data from kafka server because caller stop it")
			return nil
		default:
			buffers <- responseLengthBuf[l:length]
		}

		if length+l == 4 {
			break
		}
		l += length
	}

	responseLength := int(binary.BigEndian.Uint32(responseLengthBuf))
	if glog.V(15) {
		glog.Infof("total response length should be %d", 4+responseLength)
	}

	readLength := 0
	for {
		buf := make([]byte, 65535)
		if timeout > 0 {
			broker.conn.SetReadDeadline(time.Now().Add(time.Duration(timeout) * time.Millisecond))
		}
		length, err := broker.conn.Read(buf)
		if err != nil {
			broker.Close()
			return err
		}

		if glog.V(15) {
			glog.Infof("read %d bytes response", length)
			glog.Info(buf[:length])
		}
		select {
		case <-ctx.Done():
			return nil
		default:
			buffers <- buf[:length]
		}

		readLength += length
		if glog.V(15) {
			glog.Infof("totally send %d/%d bytes to fetch response payload", readLength+4, responseLength+4)
		}
		if readLength > responseLength {
			return errors.New("fetch more data than needed while read fetch response")
		}
		if readLength == responseLength {
			if glog.V(15) {
				glog.Info("read enough data, return")
			}
			return nil
		}
	}
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
	var version uint16 = 1
	metadataRequest := NewMetadataRequest(clientID, version, topics)

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

func (broker *Broker) requestFetchStreamingly(ctx context.Context, fetchRequest *FetchRequest, buffers chan []byte) (err error) {
	broker.mux.Lock()
	defer broker.mux.Unlock()
	//defer close(buffers)
	defer func() {
		buffers <- nil
	}()

	if err = broker.ensureOpen(); err != nil {
		return err
	}

	broker.correlationID++

	fetchRequest.SetCorrelationID(broker.correlationID)
	payload := fetchRequest.Encode(broker.getHighestAvailableAPIVersion(API_FetchRequest))

	timeout := broker.config.TimeoutMS
	if len(broker.config.TimeoutMSForEachAPI) > int(fetchRequest.API()) {
		timeout = broker.config.TimeoutMSForEachAPI[fetchRequest.API()]
	}

	return broker.requestStreamingly(ctx, payload, buffers, timeout)
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
	joinGroupRequest := NewJoinGroupRequest(1, clientID)
	joinGroupRequest.GroupID = groupID
	joinGroupRequest.SessionTimeout = sessionTimeoutMS
	joinGroupRequest.RebalanceTimeout = 60000
	joinGroupRequest.MemberID = memberID
	joinGroupRequest.ProtocolType = protocolType
	joinGroupRequest.AddGroupProtocal(&GroupProtocol{"range", []byte{}})
	joinGroupRequest.GroupProtocols = gps

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
