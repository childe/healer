package healer

import (
	"flag"
	"testing"
)

var (
	brokerAddress = flag.String("broker", "127.0.0.1:9092", "<hostname:port,...,hostname:port> The comma separated list of brokers in the Kafka cluster. (default: 127.0.0.1:9092)")
	brokers       = flag.String("brokers", "127.0.0.1:9092", "<hostname:port,...,hostname:port> The comma separated list of brokers in the Kafka cluster. (default: 127.0.0.1:9092)")
)

func init() {
	flag.Parse()
}

func TestNewBroker(t *testing.T) {
	_, err := NewBroker(*brokerAddress, "healer", -1)
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	}

	_, err = NewBroker(*brokerAddress, "", -1)
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	}

	_, err = NewBroker(*brokerAddress, "healer", 0)
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	}

	_, err = NewBroker(*brokerAddress, "", 0)
	if err != nil {
		t.Errorf("new broker from %s error:%s", *brokerAddress, err)
	}

	_, err = NewBroker("127.0.0.1:10000", "", 0)
	if err == nil {
		t.Errorf("it should not get new broker from 127.0.0.1:10000")
	}
}
