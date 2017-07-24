package healer

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/golang/glog"
)

type Brokers struct {
	brokers []*Broker
}

func NewBrokers(brokerList string, clientID string) (*Brokers, error) {
	availableBroker := ""
	brokers := &Brokers{}
	brokers.brokers = make([]*Broker, 0)
	for _, brokerAddr := range strings.Split(brokerList, ",") {
		broker, err := NewBroker(brokerAddr, clientID)
		if err != nil {
			glog.Infof("init broker from %s error:%s", brokerAddr, err)
		} else {
			brokers.brokers = append(brokers.brokers, broker)
			availableBroker = brokerAddr
			break
		}
	}
	if availableBroker == "" {
		return nil, fmt.Errorf("could not get any available broker from %s", brokerList)
	}

	// get all brokers
	topic := ""
	metadataResponse, err := brokers.brokers[0].RequestMetaData(&topic)
	if err != nil {
		glog.Infof("could not get metadata from %s:%s", brokers.brokers[0].address, err)
		return brokers, nil
	}

	if glog.V(10) {
		s, err := json.MarshalIndent(metadataResponse, "", "  ")
		if err != nil {
			glog.Infof("failed to marshal brokers info from metadata: %s", err)
		} else {
			glog.Infof("brokers info from metadata: %s", s)
		}
	}

	for _, brokerInfo := range metadataResponse.Brokers {
		brokerAddr := fmt.Sprintf("%s:%d", brokerInfo.Host, brokerInfo.Port)
		if brokerAddr == availableBroker {
			continue
		}
		broker, err := NewBroker(brokerAddr, clientID)
		if err != nil {
			glog.Infof("init broker from %s error:%s", brokerAddr, err)
		} else {
			brokers.brokers = append(brokers.brokers, broker)
		}
	}

	glog.Infof("got %d brokers", len(brokers.brokers))

	if glog.V(5) {
		addresses := make([]string, len(brokers.brokers))
		for i, broker := range brokers.brokers {
			addresses[i] = broker.address
		}
		glog.Infof("all brokers: %s", strings.Join(addresses, ","))
	}

	return brokers, nil
}

func (brokers *Brokers) RequestMetaData(topic *string) (*MetadataResponse, error) {
	for _, broker := range brokers.brokers {
		metadataResponse, err := broker.RequestMetaData(topic)
		if err != nil {
			glog.Infof("could not get metadata from %s:%s", broker.address, err)
		} else {
			return metadataResponse, nil
		}
	}

	return nil, fmt.Errorf("could not get metadata from all brokers")
}

// GetOffset return the offset values array from server
func (brokers *Brokers) RequestOffset(topic *string, partitionID int32, timeValue int64, offsets uint32) (*OffsetResponse, error) {
	for _, broker := range brokers.brokers {
		offsetsResponse, err := broker.RequestOffsets(topic, partitionID, timeValue, offsets)
		if err != nil {
			glog.Infof("could not get offsets from %s:%s", broker.address, err)
		} else {
			return offsetsResponse, nil
		}
	}

	return nil, fmt.Errorf("could not get offsets from all brokers")
}
