package healer

import (
	"flag"
	"testing"
)

var (
	brokerAddress = flag.String("broker", "127.0.0.1:9092", "<hostname:port,...,hostname:port> The comma separated list of brokers in the Kafka cluster. (default: 127.0.0.1:9092)")
	brokers       = flag.String("brokers", "127.0.0.1:9092", "<hostname:port,...,hostname:port> The comma separated list of brokers in the Kafka cluster. (default: 127.0.0.1:9092)")
	topic         = flag.String("topic", "test", "topic name")
	groupID       = flag.String("groupid", "healer", "groupid")
)

func init() {
	flag.Parse()
}

func TestNewBroker(t *testing.T) {
	_, err := NewBroker(*brokerAddress, "healer", -1)
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	} else {
		t.Logf("got new broker from %s %s %d", *brokerAddress, "healer", -1)
	}

	_, err = NewBroker(*brokerAddress, "", -1)
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	} else {
		t.Logf("got new broker from %s %s %d", *brokerAddress, "", -1)
	}

	_, err = NewBroker(*brokerAddress, "healer", 0)
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	} else {
		t.Logf("got new broker from %s %s %d", *brokerAddress, "healer", 0)
	}

	_, err = NewBroker(*brokerAddress, "", 0)
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	} else {
		t.Logf("got new broker from %s %s %d", *brokerAddress, "", -1)
	}

	_, err = NewBroker("127.0.0.1:21010", "", 0)
	if err == nil {
		t.Errorf("it should not get new broker from 127.0.0.1:10000")
	} else {
		t.Logf("got new broker from %s %s %d", *brokerAddress, "", 0)
	}
}

func TestRequestFindCoordinator(t *testing.T) {
	broker, err := NewBroker(*brokerAddress, "healer", -1)
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	} else {
		t.Logf("got new broker from %s %s %d", *brokerAddress, "healer", -1)
	}

	_, err = broker.requestFindCoordinator(*groupID)
	if err != nil {
		t.Errorf("request FindCoordinator for groupID[%s] error:%s", *groupID, err)
	} else {
		t.Logf("reqeust FindCoordinator for groupID[%s] OK", *groupID)
	}
}

func TestRequestApiVersions(t *testing.T) {
	broker, err := NewBroker(*brokerAddress, "healer", -1)
	defer broker.Close()
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	}

	apiVersionsResponse, err := broker.requestApiVersions()
	if apiVersionsResponse.ErrorCode != 0 {
		t.Errorf("apiVersionsResponse error code is %d", apiVersionsResponse.ErrorCode)
	} else {
		t.Log("got apiversions response")
	}

	for _, ApiVersion := range apiVersionsResponse.ApiVersions {
		t.Logf("broker %s apiKey is %d, minVersion is %d, maxVersion is %d", *brokerAddress, ApiVersion.apiKey, ApiVersion.minVersion, ApiVersion.maxVersion)
	}
}
