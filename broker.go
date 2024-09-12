package healer

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"time"
)

type Broker struct {
	config  *BrokerConfig
	address string
	nodeID  int32

	conn        net.Conn
	apiVersions []APIVersion

	correlationID uint32

	mux       sync.Mutex
	closeLock sync.Mutex
}

var (
	errTLSConfig = errors.New("tls is enabled but either cert or key or ca is not set")
)

// NewBroker is only called in NewBrokers, user must always init a Brokers instance by NewBrokers
func NewBroker(address string, nodeID int32, config *BrokerConfig) (*Broker, error) {
	broker := &Broker{
		config:  config,
		address: address,
		nodeID:  nodeID,

		correlationID: 0,
	}
	if err := broker.createConnAndAuth(); err != nil {
		return nil, err
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

func (broker *Broker) createConn() error {
	if conn, err := newConn(broker.GetAddress(), broker.config); err != nil {
		return err
	} else {
		broker.conn = conn
		return nil
	}
}

// create a new connection to the broker, and then do the sasl authenticate if needed
func (broker *Broker) createConnAndAuth() error {
	if err := broker.createConn(); err != nil {
		return err
	}

	clientID := "healer-init"
	apiVersionsResponse, err := broker.requestAPIVersions(clientID)
	if err != nil {
		return fmt.Errorf("failed to request api versions after creating new conn: %w", err)
	}
	broker.apiVersions = apiVersionsResponse.APIVersions

	var versions string
	for i, v := range broker.apiVersions {
		if i+1 == len(broker.apiVersions) {
			versions += fmt.Sprintf("%s:%d-%d", v.apiKey.String(), v.minVersion, v.maxVersion)
		} else {
			versions += fmt.Sprintf("%s:%d-%d,", v.apiKey.String(), v.minVersion, v.maxVersion)
		}
	}
	logger.V(5).Info("broker api versions", "broker", broker.address, "versions", versions)

	if broker.config.Sasl.Mechanism != "" {
		if err := broker.sendSaslAuthenticate(); err != nil {
			logger.Error(err, "sasl authenticate failed", "nodeID", broker.nodeID, "adddress", broker.address)
			return err
		}
	}
	return nil
}

func newConn(address string, config *BrokerConfig) (net.Conn, error) {
	logger.V(5).Info("dial to create connection to a broker", "address", address)
	dialer := net.Dialer{
		Timeout:   time.Millisecond * time.Duration(config.Net.ConnectTimeoutMS),
		KeepAlive: time.Millisecond * time.Duration(config.Net.KeepAliveMS),
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

	caCert, err := os.ReadFile(tlsConfig.CA)
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
	var (
		clientID  = "healer-sals-authenticate"
		mechanism = broker.config.Sasl.Mechanism
		user      = broker.config.Sasl.User
		password  = broker.config.Sasl.Password
	)

	saslHandShakeRequest := NewSaslHandShakeRequest(clientID, mechanism)
	resp, err := broker.RequestAndGet(saslHandShakeRequest)
	if err != nil {
		return err
	}
	if resp.Error() != nil {
		return resp.Error()
	}

	// authenticate
	saslAuthenticateRequest := NewSaslAuthenticateRequest(clientID, user, password, mechanism)
	resp, err = broker.RequestAndGet(saslAuthenticateRequest)
	if err != nil {
		return err
	}
	if resp.Error() != nil {
		return resp.Error()
	}
	return nil
}

// GetAddress returns the broker address
func (broker *Broker) GetAddress() string {
	return broker.address
}

func (broker *Broker) String() string {
	return fmt.Sprintf("[%d]%s", broker.nodeID, broker.address)
}

// Close closes the connection to the broker
func (broker *Broker) Close() {
	logger.Info("close broker", "broker", broker.String())

	broker.closeLock.Lock()
	defer broker.closeLock.Unlock()

	if broker.conn != nil {
		broker.conn.Close()
		broker.conn = nil
	}
}
func (broker *Broker) ensureOpen() (err error) {
	if broker.conn != nil {
		return nil
	}
	return broker.createConnAndAuth()
}

// Request sends a request to the broker and returns a readParser
// user should call RequestAndGet() to get the response
func (broker *Broker) Request(r Request) (ReadParser, error) {
	broker.correlationID++
	r.SetCorrelationID(broker.correlationID)
	timeout := broker.config.Net.TimeoutMS
	if len(broker.config.Net.TimeoutMSForEachAPI) > int(r.API()) {
		if broker.config.Net.TimeoutMSForEachAPI[r.API()] > 0 {
			timeout = broker.config.Net.TimeoutMSForEachAPI[r.API()]
		}
	}
	version := broker.getHighestAvailableAPIVersion(r.API())
	r.SetVersion(version)
	rp, err := broker.request(r.Encode(version), timeout)
	if err != nil {
		return nil, fmt.Errorf("request of %d(%d) to %s error: %w", r.API(), version, broker.GetAddress(), err)
	}
	rp.version = version
	rp.api = r.API()

	return rp, err
}

// RequestAndGet sends a request to the broker and returns the response
func (broker *Broker) RequestAndGet(r Request) (resp Response, err error) {
	broker.mux.Lock()
	defer broker.mux.Unlock()

	if err := broker.ensureOpen(); err != nil {
		return nil, err
	}

	defer func() {
		if os.IsTimeout(err) || errors.Is(err, io.EOF) || errors.Is(err, syscall.EPIPE) {
			broker.Close()
		}
	}()

	rp, err := broker.Request(r)
	if err != nil {
		return nil, err
	}

	resp, err = rp.ReadAndParse()
	if err != nil {
		return nil, err
	}
	return resp, resp.Error()
}

func (broker *Broker) request(payload []byte, timeout int) (defaultReadParser, error) {
	logger.V(5).Info("send request", "src", broker.conn.LocalAddr(), "dst", broker.conn.RemoteAddr())
	api := ApiKey(binary.BigEndian.Uint16(payload[4:]))
	apiVersion := binary.BigEndian.Uint16(payload[6:])
	correlationID := binary.BigEndian.Uint32(payload[8:])
	logger.V(5).Info("request info", "length", len(payload), "api", api, "apiVersion", apiVersion, "correlationID", correlationID, "timeout", timeout)

	if _, err := io.Copy(broker.conn, bytes.NewBuffer(payload)); err != nil {
		return defaultReadParser{}, err
	}

	rp := defaultReadParser{
		broker:  broker,
		timeout: timeout,
	}
	return rp, nil
}

func (broker *Broker) requestStreamingly(payload []byte, timeout int) (r io.Reader, responseLength uint32, err error) {
	defer func() {
		if err != nil {
			broker.Close()
		}
	}()

	logger.V(5).Info("send request", "src", broker.conn.LocalAddr(), "dst", broker.conn.RemoteAddr())
	api := ApiKey(binary.BigEndian.Uint16(payload[4:]))
	apiVersion := binary.BigEndian.Uint16(payload[6:])
	correlationID := binary.BigEndian.Uint32(payload[8:])
	logger.V(5).Info("request info", "length", len(payload), "api", api, "apiVersion", apiVersion, "correlationID", correlationID, "timeout", timeout)

	io.Copy(broker.conn, bytes.NewBuffer(payload))

	responseLengthBuf := make([]byte, 4)
	if timeout > 0 {
		broker.conn.SetReadDeadline(time.Now().Add(time.Duration(timeout) * time.Millisecond))
	}
	n, err := broker.conn.Read(responseLengthBuf)
	if err != nil {
		return nil, responseLength, err
	}
	if n != 4 {
		return nil, responseLength, errShortRead
	}

	responseLength = binary.BigEndian.Uint32(responseLengthBuf)
	logger.V(5).Info("got responseLength", "responseLength", responseLength)

	reader := &io.LimitedReader{
		R: broker.conn,
		N: int64(responseLength),
	}
	return reader, responseLength, nil
}

// used in broker init and reopen after close.
// not use RequeAndResponse to avoid dead lock
func (broker *Broker) requestAPIVersions(clientID string) (r APIVersionsResponse, err error) {
	apiVersionRequest := NewApiVersionsRequest(clientID)
	rp, err := broker.Request(apiVersionRequest)
	if err != nil {
		return r, err
	}

	resp, err := rp.ReadAndParse()
	if err != nil {
		return r, err
	}
	if err = resp.Error(); err != nil {
		return r, err
	}
	return resp.(APIVersionsResponse), nil
}

func (broker *Broker) RequestListGroups(clientID string) (r ListGroupsResponse, err error) {
	request := NewListGroupsRequest(clientID)

	resp, err := broker.RequestAndGet(request)
	if v, ok := resp.(ListGroupsResponse); ok {
		return v, err
	}
	return r, err
}

func (broker *Broker) requestMetaData(clientID string, topics []string) (r MetadataResponse, err error) {
	metadataRequest := NewMetadataRequest(clientID, topics)

	resp, err := broker.RequestAndGet(metadataRequest)
	if v, ok := resp.(MetadataResponse); ok {
		return v, err
	}
	return r, err
}

// RequestOffsets return the offset values array from ther broker. all partitionID in partitionIDs must be in THIS broker
func (broker *Broker) requestOffsets(clientID, topic string, partitionIDs []int32, timeValue int64, offsets uint32) (r OffsetsResponse, err error) {
	offsetsRequest := NewOffsetsRequest(topic, partitionIDs, timeValue, offsets, clientID)

	resp, err := broker.RequestAndGet(offsetsRequest)
	if v, ok := resp.(OffsetsResponse); ok {
		return v, err
	}
	return r, err
}

func (broker *Broker) requestFetchStreamingly(fetchRequest *FetchRequest) (r io.Reader, responseLength uint32, err error) {
	if err := broker.ensureOpen(); err != nil {
		return nil, 0, err
	}

	broker.mux.Lock()
	defer broker.mux.Unlock()

	broker.correlationID++

	fetchRequest.SetCorrelationID(broker.correlationID)
	payload := fetchRequest.Encode(broker.getHighestAvailableAPIVersion(API_FetchRequest))

	timeout := broker.config.Net.TimeoutMS
	if len(broker.config.Net.TimeoutMSForEachAPI) > int(fetchRequest.API()) {
		timeout = broker.config.Net.TimeoutMSForEachAPI[fetchRequest.API()]
	}

	return broker.requestStreamingly(payload, timeout)
}

func (broker *Broker) findCoordinator(clientID, groupID string) (r FindCoordinatorResponse, err error) {
	request := NewFindCoordinatorRequest(clientID, groupID)

	resp, err := broker.RequestAndGet(request)
	if v, ok := resp.(FindCoordinatorResponse); ok {
		return v, err
	}
	return r, err
}

func (broker *Broker) requestJoinGroup(clientID, groupID string, sessionTimeoutMS int32, memberID, protocolType string, gps []*GroupProtocol) (r JoinGroupResponse, err error) {
	joinGroupRequest := NewJoinGroupRequest(1, clientID)
	joinGroupRequest.GroupID = groupID
	joinGroupRequest.SessionTimeout = sessionTimeoutMS
	joinGroupRequest.RebalanceTimeout = 60000
	joinGroupRequest.MemberID = memberID
	joinGroupRequest.ProtocolType = protocolType
	joinGroupRequest.AddGroupProtocal(&GroupProtocol{"range", []byte{}})
	joinGroupRequest.GroupProtocols = gps

	resp, err := broker.RequestAndGet(joinGroupRequest)
	if v, ok := resp.(JoinGroupResponse); ok {
		return v, err
	}
	return r, err
}

func (broker *Broker) requestSyncGroup(clientID, groupID string, generationID int32, memberID string, groupAssignment GroupAssignment) (r SyncGroupResponse, err error) {
	syncGroupRequest := NewSyncGroupRequest(clientID, groupID, generationID, memberID, groupAssignment)

	resp, err := broker.RequestAndGet(syncGroupRequest)
	if v, ok := resp.(SyncGroupResponse); ok {
		return v, err
	}
	return r, err
}

func (broker *Broker) requestHeartbeat(clientID, groupID string, generationID int32, memberID string) (r HeartbeatResponse, err error) {
	req := NewHeartbeatRequest(clientID, groupID, generationID, memberID)

	resp, err := broker.RequestAndGet(req)
	if v, ok := resp.(HeartbeatResponse); ok {
		return v, err
	}
	return r, err
}

func (broker *Broker) requestLeaveGroup(clientID, groupID string, memberID string) (r LeaveGroupResponse, err error) {
	leaveReq := NewLeaveGroupRequest(clientID, groupID, memberID)
	resp, err := broker.RequestAndGet(leaveReq)
	return resp.(LeaveGroupResponse), err
}
