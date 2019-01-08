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
		mechanism string = "PLAIN"

		payload []byte
		err     error
	)

	broker, _ := healer.NewBroker(*brokerAddress, -1, healer.DefaultBrokerConfig())

	saslHandShakeRequest := healer.NewSaslHandShakeRequest(clientID, mechanism)

	payload = saslHandShakeRequest.Encode()
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

	// test offsets request
	var (
		partitionID int32  = 0
		timeValue   int64  = 0
		offsets     uint32 = 10
		topic       string = "test"
	)

	offsetsRequest := healer.NewOffsetsRequest(topic, []int32{partitionID}, timeValue, offsets, clientID)

	payload = offsetsRequest.Encode()
	if len(payload) != 54 {
		t.Error("offsets request payload length should be 54")
	}
	responseBuf, err = broker.Request(offsetsRequest)
	if err != nil {
		t.Errorf("requet offsetcommit error:%s", err)
	}

	t.Logf("%v", responseBuf)
	offsetsResponse, err := healer.NewOffsetsResponse(responseBuf)
	if err != nil {
		t.Errorf("decode offsets response error:%s", err)
	}
	t.Logf("%v", offsetsResponse)

}
