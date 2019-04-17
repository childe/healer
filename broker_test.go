package healer

import (
	"flag"
	"fmt"
	"os"
	"testing"
)

var (
	brokerConfig = DefaultBrokerConfig()

	brokerAddress = flag.String("broker", "127.0.0.1:9092", "<hostname:port,...,hostname:port> The comma separated list of brokers in the Kafka cluster. (default: 127.0.0.1:9092)")
	brokersList   = flag.String("brokers", "127.0.0.1:9092", "<hostname:port,...,hostname:port> The comma separated list of brokers in the Kafka cluster. (default: 127.0.0.1:9092)")
	topic         = flag.String("topic", "test", "topic name")
	groupID       = flag.String("group", "healer", "groupid")
	clientID      = flag.String("client", "healer", "groupid")
)

func TestMain(m *testing.M) {
	flag.Parse()

	if err := createTopics(); err != nil {
		fmt.Println(err)
	} else {
		os.Exit(m.Run())
	}
}

func createTopics() error {
	brokers, err := NewBrokers(*brokersList)
	if err != nil {
		return err
	}

	controller, err := brokers.GetBroker(brokers.Controller())
	if err != nil {
		return err
	}

	timeout := int32(30000)
	r := NewCreateTopicsRequest(*clientID, timeout)

	partitions := int32(4)
	replicationFactor := int16(2)
	r.AddTopic(*topic, partitions, replicationFactor)

	payload, err := controller.Request(r)
	if err != nil {
		return err
	}

	NewCreateTopicsResponse(payload)
	return nil
}

func TestNewBroker(t *testing.T) {
	_, err := NewBroker(*brokerAddress, -1, brokerConfig)
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	} else {
		t.Logf("got new broker from %s %s %d", *brokerAddress, "healer", -1)
	}

	brokerConfig.TimeoutMS = 0
	_, err = NewBroker(*brokerAddress, -1, brokerConfig)
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	} else {
		t.Logf("got new broker from %s %s %d", *brokerAddress, "", -1)
	}

	brokerConfig.ConnectTimeoutMS = 0
	_, err = NewBroker(*brokerAddress, 0, brokerConfig)
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	} else {
		t.Logf("got new broker from %s %s %d", *brokerAddress, "healer", 0)
	}

	_, err = NewBroker("127.0.0.1:21010", 0, brokerConfig)
	if err == nil {
		t.Errorf("it should not get new broker from 127.0.0.1:10000")
	} else {
		t.Logf("got new broker from %s %s %d", *brokerAddress, "", 0)
	}
}

func TestRequestApiVersions(t *testing.T) {
	return
	broker, err := NewBroker(*brokerAddress, -1, brokerConfig)
	defer broker.Close()
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	}

	apiVersionsResponse, err := broker.requestApiVersions(*clientID)
	if apiVersionsResponse.ErrorCode != 0 {
		t.Errorf("apiVersionsResponse error code is %d", apiVersionsResponse.ErrorCode)
	} else {
		t.Log("got apiversions response")
	}

	for _, ApiVersion := range apiVersionsResponse.ApiVersions {
		t.Logf("broker %s apiKey is %d, minVersion is %d, maxVersion is %d", *brokerAddress, ApiVersion.apiKey, ApiVersion.minVersion, ApiVersion.maxVersion)
	}
}
