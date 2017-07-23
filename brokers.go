package healer

import (
	"fmt"
	"strings"

	"github.com/golang/glog"
)

type Brokers struct {
	brokers []*Broker
}

func GetBrokers(brokerList string) (*Brokers, error) {
	hasAvailableBroker := false
	brokers := &Brokers{}
	brokers.brokers = make([]*Broker, 0)
	for _, brokerAddr := range strings.Split(brokerList, ",") {
		broker, err := GetBroker(brokerAddr)
		if err != nil {
			glog.Infof("init broker error from %s:%s", brokerAddr, err)
		} else {
			brokers.brokers = append(brokers.brokers, broker)
			hasAvailableBroker = true
			break
		}
	}
	if hasAvailableBroker == false {
		return nil, fmt.Errorf("could not get any available broker from %s", brokerList)
	}

	// TODO get all brokers
	return brokers, nil
}
