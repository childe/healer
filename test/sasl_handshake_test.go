package test

import (
	"flag"
	"testing"

	"github.com/childe/healer"
)

var (
	brokerAddress = flag.String("broker", "127.0.0.1:9092", "<hostname:port,...,hostname:port> The comma separated list of brokers in the Kafka cluster. (default: 127.0.0.1:9092)")
)

func init() {
	flag.Parse()
}

func TestGenSaslHandShakeRequest(t *testing.T) {
	var (
		clientID  string = "healer"
		mechanism string = ""
	)

	broker, _ := healer.NewBroker(*brokerAddress, -1, healer.DefaultBrokerConfig())

	saslHandShakeRequest := healer.NewSaslHandShakeRequest(clientID, mechanism)

	payload := saslHandShakeRequest.Encode()
	if len(payload) != 22 {
		t.Error("SaslHandShakeRequest payload length should be 22")
	}

	responseBuf, err := broker.Request(saslHandShakeRequest)
	if err != nil {
		t.Errorf("requet offsetcommit error:%s", err)
	}

	response, err := healer.NewSaslHandshakeResponse(responseBuf)
	if err != nil {
		t.Errorf("decode SaslHandshake response error:%s", err)
	}
	t.Logf("SaslHandshake mechanisms: %v", response.EnabledMechanisms)
}
