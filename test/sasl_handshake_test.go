package test

import (
	"flag"
	"testing"

	"github.com/childe/healer"
)

var (
	brokerAddress = flag.String("broker", "127.0.0.1:9092", "<hostname:port,...,hostname:port> The comma separated list of brokers in the Kafka cluster. (default: 127.0.0.1:9092)")
)

func TestGenSaslHandShakeRequest(t *testing.T) {
	var (
		clientID  string = "healer"
		mechanism string = "PLAIN"

		payload []byte
		err     error
	)

	broker, _ := healer.NewBroker(*brokerAddress, -1, healer.DefaultBrokerConfig())

	saslHandShakeRequest := healer.NewSaslHandShakeRequest(clientID, mechanism)

	payload = saslHandShakeRequest.Encode(0)
	if len(payload) != 27 {
		t.Error("SaslHandShakeRequest payload length should be 27")
	}

	payload, err = broker.Request(saslHandShakeRequest)
	if err != nil {
		t.Errorf("requet SaslHandshake error:%s", err)
	}

	response, err := healer.NewSaslHandshakeResponse(payload)
	if err != nil {
		t.Errorf("decode SaslHandshake response error:%s", err)
	}
	t.Logf("SaslHandshake ErrorCode: %v", response.ErrorCode)
	t.Logf("SaslHandshake mechanisms: %v", response.EnabledMechanisms)

	// authenticate
	var (
		user     string = "admin"
		password string = "admin-secret"
	)
	saslAuthenticateRequest := healer.NewSaslAuthenticateRequest(clientID, user, password, "plain")
	payload, err = broker.Request(saslAuthenticateRequest)
	if err != nil {
		t.Errorf("requet SaslAuthenticate error:%s", err)
	}
	saslAuthenticateResponse, err := healer.NewSaslAuthenticateResponse(payload)
	if err != nil {
		t.Errorf("decode SaslAuthenticateResponse error:%s", err)
	}
	t.Logf("ErrorCode %d", saslAuthenticateResponse.ErrorCode)
	t.Logf("ErrorMessage %s", saslAuthenticateResponse.ErrorMessage)

	// test offsets request
	var (
		partitionID int32  = 0
		timeValue   int64  = 0
		offsets     uint32 = 10
		topic       string = "test"
	)

	offsetsRequest := healer.NewOffsetsRequest(topic, []int32{partitionID}, timeValue, offsets, clientID)

	payload = offsetsRequest.Encode(0)
	if len(payload) != 54 {
		t.Error("offsets request payload length should be 54")
	}
	payload, err = broker.Request(offsetsRequest)
	if err != nil {
		t.Errorf("requet offsetcommit error:%s", err)
	}

	offsetsResponse, err := healer.NewOffsetsResponse(payload)
	if err != nil {
		t.Errorf("decode offsets response error:%s", err)
	}
	t.Logf("%+v", offsetsResponse)
}
