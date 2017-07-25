package healer

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/golang/glog"
)

type Brokers struct {
	brokers map[int32]*Broker
}

func getAllBrokersFromOne(broker *Broker, clientID string) (*Brokers, error) {
	brokers := &Brokers{}
	brokers.brokers = make(map[int32]*Broker)

	topic := ""
	metadataResponse, err := broker.RequestMetaData(&topic)
	if err != nil {
		glog.Infof("could not get metadata from %s:%s", broker.address, err)
		return nil, err
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
		broker, err := NewBroker(brokerAddr, clientID, brokerInfo.NodeId)
		if err != nil {
			glog.Infof("init broker from %s error:%s", brokerAddr, err)
		} else {
			brokers.brokers[brokerInfo.NodeId] = broker
		}
	}

	glog.Infof("got %d brokers", len(brokers.brokers))

	if glog.V(5) {
		addresses := make([]string, len(brokers.brokers))
		i := 0
		for _, broker := range brokers.brokers {
			addresses[i] = broker.address
			i++
		}
		glog.Infof("all brokers: %s", strings.Join(addresses, ","))
	}

	return brokers, nil
}

func NewBrokers(brokerList string, clientID string) (*Brokers, error) {
	for _, brokerAddr := range strings.Split(brokerList, ",") {
		broker, err := NewBroker(brokerAddr, clientID, -1)
		if err != nil {
			glog.Infof("init broker from %s error:%s", brokerAddr, err)
		} else {
			brokers, err := getAllBrokersFromOne(broker, clientID)
			if err != nil {
				glog.Infof("could not get broker list from %s:%s", broker.address, err)
			} else {
				return brokers, nil
			}
		}
	}
	return nil, fmt.Errorf("could not get any available broker from %s", brokerList)

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
func (brokers *Brokers) RequestOffsets(topic *string, partitionID int32, timeValue int64, offsets uint32) ([]*OffsetResponse, error) {
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
