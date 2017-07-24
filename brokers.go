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

func NewBrokers(brokerList string) (*Brokers, error) {
	hasAvailableBroker := false
	brokers := &Brokers{}
	brokers.brokers = make([]*Broker, 0)
	for _, brokerAddr := range strings.Split(brokerList, ",") {
		broker, err := NewBroker(brokerAddr)
		if err != nil {
			glog.Infof("init broker from %s error:%s", brokerAddr, err)
		} else {
			brokers.brokers = append(brokers.brokers, broker)
			hasAvailableBroker = true
			break
		}
	}
	if hasAvailableBroker == false {
		return nil, fmt.Errorf("could not get any available broker from %s", brokerList)
	}

	// get all brokers
	metadataResponse, err := brokers.brokers[0].RequestMetaData("")
	if err != nil {
		glog.Infof("could not get metadata from %s:%s", brokers.brokers[0].address, err)
		return brokers, nil
	}

	if glog.V(10) {
		s, err := json.MarshalIndent(metadataResponse, "", "  ")
		if err != nil {
			glog.Infof("brokers info from metadata: %s", s)
		} else {
			glog.Infof("failed to marshal brokers info from metadata: %s", err)
		}
	}

	for _, brokerInfo := range metadataResponse.Brokers {
		brokerAddr := fmt.Fprint("%s:%d", brokerInfo.Host, brokerInfo.Port)
		broker, err := NewBroker(brokerAddr)
		if err != nil {
			glog.Infof("init broker from %s error:%s", brokerAddr, err)
		} else {
			brokers.brokers = append(brokers.brokers, broker)
			break
		}
	}

	if glog.V(10) {
		addresses := make([]string, len(brokers.brokers))
		for i, broker := range brokers.brokers {
			addresses[i] = broker.address
		}
		glog.Infof("all brokers: %s", strings.Join(addresses))
	}

	return brokers, nil
}
