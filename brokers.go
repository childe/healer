//TODO referesh metadata when running into error

package healer

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
)

type Brokers struct {
	config           *BrokerConfig
	bootstrapServers string

	brokersInfo  map[int32]*BrokerInfo
	brokers      map[int32]*Broker
	controllerID int32

	mutex sync.Locker

	closeChan chan bool
}

// Close close all brokers
func (brokers *Brokers) Close() {
	brokers.closeChan <- true
	for _, broker := range brokers.brokers {
		broker.Close()
	}
}

// NewBrokersWithConfig create a new broker with config
func NewBrokersWithConfig(bootstrapServers string, config *BrokerConfig) (*Brokers, error) {
	clientID := "healer-newbrokers"
	for _, brokerAddr := range strings.Split(bootstrapServers, ",") {
		broker, err := NewBroker(brokerAddr, -1, config)

		// TODO conn not established?
		if err != nil {
			glog.Infof("init broker from %s error:%s", brokerAddr, err)
		} else {
			brokers, err := newBrokersFromOne(broker, clientID, config)
			if err != nil {
				glog.Infof("could not get broker list from %s:%s", broker.GetAddress(), err)
			} else {
				go func() {
					ticker := time.NewTicker(time.Duration(config.MetadataRefreshIntervalMS) * time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							if !brokers.refreshMetadata() {
								glog.Error("refresh metadata error")
							}
						case <-brokers.closeChan:
							return
						}
					}
				}()
				brokers.bootstrapServers = bootstrapServers
				return brokers, nil
			}
		}
	}
	return nil, fmt.Errorf("could not get any available broker from %s", bootstrapServers)
}

// NewBrokers create a new broker with default config
func NewBrokers(bootstrapServers string) (*Brokers, error) {
	return NewBrokersWithConfig(bootstrapServers, DefaultBrokerConfig())
}

// get all brokers meda info from MetaData api
// it DON'T create connection to each broker
func newBrokersFromOne(broker *Broker, clientID string, config *BrokerConfig) (*Brokers, error) {
	brokers := &Brokers{
		config:      config,
		brokersInfo: make(map[int32]*BrokerInfo),
		brokers:     make(map[int32]*Broker),
		mutex:       &sync.Mutex{},
		closeChan:   make(chan bool, 0),
	}

	topics := make([]string, 0)
	metadataResponse, err := broker.requestMetaData(clientID, topics)
	if err != nil {
		return nil, err
	}
	if len(metadataResponse.Brokers) == 0 {
		return nil, errors.New("no broers in getmetadata response")
	}

	brokers.mutex.Lock()
	defer brokers.mutex.Unlock()
	brokers.controllerID = metadataResponse.ControllerID
	for _, brokerInfo := range metadataResponse.Brokers {
		brokers.brokersInfo[brokerInfo.NodeID] = brokerInfo
		if broker.GetAddress() == fmt.Sprintf("%s:%d", brokerInfo.Host, brokerInfo.Port) {
			brokers.brokers[brokerInfo.NodeID] = broker
		}
	}

	if glog.V(5) {
		glog.Infof("got %d brokers", len(brokers.brokersInfo))
		for nodeID, broker := range brokers.brokersInfo {
			glog.Infof("%d %s:%d", nodeID, broker.Host, broker.Port)
		}
	}

	return brokers, nil
}

// Controller return controller broker id
func (brokers *Brokers) Controller() int32 {
	return brokers.controllerID
}

func (brokers *Brokers) refreshMetadata() bool {
	topics := make([]string, 0)
	clientID := "healer-refresh-metadata"

	// from origianl bootstrapServers
	for _, brokerAddr := range strings.Split(brokers.bootstrapServers, ",") {
		broker, err := NewBroker(brokerAddr, -1, brokers.config)
		if err != nil {
			glog.Errorf("create broker[%s] error: %s", brokerAddr, err)
			continue
		}

		metadataResponse, err := broker.requestMetaData(clientID, topics)
		if len(metadataResponse.Brokers) == 0 {
			glog.Errorf("request metadata error: %s", err)
			broker.Close()
			continue
		}

		brokersInfo := make(map[int32]*BrokerInfo)
		for _, brokerInfo := range metadataResponse.Brokers {
			brokersInfo[brokerInfo.NodeID] = brokerInfo
		}
		brokers.brokersInfo = brokersInfo

		glog.Infof("got %d brokers", len(brokers.brokersInfo))
		if glog.V(2) {
			for nodeID, broker := range brokers.brokersInfo {
				glog.Infof("%d %s:%d", nodeID, broker.Host, broker.Port)
			}
		}
		return true
	}

	glog.Info("update metadata from latest brokersInfo")
	// from latest brokersinfo
	for _, brokerInfo := range brokers.brokersInfo {
		brokerAddr := fmt.Sprintf("%s:%d", brokerInfo.Host, brokerInfo.Port)
		broker, err := NewBroker(brokerAddr, -1, brokers.config)
		if err != nil {
			glog.Errorf("create broker[%s] error: %s", brokerAddr, err)
			continue
		}

		metadataResponse, err := broker.requestMetaData(clientID, topics)
		if len(metadataResponse.Brokers) == 0 {
			glog.Errorf("request metadata error: %s", err)
			continue
		}

		brokersInfo := make(map[int32]*BrokerInfo)
		for _, brokerInfo := range metadataResponse.Brokers {
			brokersInfo[brokerInfo.NodeID] = brokerInfo
		}
		brokers.brokersInfo = brokersInfo

		glog.Infof("got %d brokers", len(brokers.brokersInfo))
		if glog.V(2) {
			for nodeID, broker := range brokers.brokersInfo {
				glog.Infof("%d %s:%d", nodeID, broker.Host, broker.Port)
			}
		}
		return true
	}
	return false
}

// TODO merge with GetBroker
func (brokers *Brokers) NewBroker(nodeID int32) (*Broker, error) {
	if nodeID == -1 {
		for nodeID, brokerInfo := range brokers.brokersInfo {
			broker, err := NewBroker(fmt.Sprintf("%s:%d", brokerInfo.Host, brokerInfo.Port), nodeID, brokers.config)
			if err == nil {
				return broker, nil
			}
			glog.Errorf("create broker %s:%d, error: %s", brokerInfo.Host, brokerInfo.Port, err)
		}
		return nil, fmt.Errorf("could not get broker from nodeID[%d]", nodeID)
	}

	if brokerInfo, ok := brokers.brokersInfo[nodeID]; ok {
		broker, err := NewBroker(fmt.Sprintf("%s:%d", brokerInfo.Host, brokerInfo.Port), brokerInfo.NodeID, brokers.config)
		if err == nil {
			return broker, nil
		} else {
			return nil, fmt.Errorf("could not init broker for node[%d](%s:%d), error: :%s", nodeID, brokerInfo.Host, brokerInfo.Port, err)
		}
	} else {
		glog.Infof("could not get broker info with nodeID[%d], referesh medadata", nodeID)
		if !brokers.refreshMetadata() {
			glog.Error("refresh metadata error")
		}
	}

	// try again after refereshing metadata
	if brokerInfo, ok := brokers.brokersInfo[nodeID]; ok {
		broker, err := NewBroker(fmt.Sprintf("%s:%d", brokerInfo.Host, brokerInfo.Port), brokerInfo.NodeID, brokers.config)
		if err == nil {
			return broker, nil
		} else {
			return nil, fmt.Errorf("could not init broker for node[%d](%s:%d):%s", nodeID, brokerInfo.Host, brokerInfo.Port, err)
		}
	} else {
		return nil, fmt.Errorf("could not get broker info with nodeID[%d]", nodeID)
	}
}

// GetBroker returns broker from cache or create a new one
func (brokers *Brokers) GetBroker(nodeID int32) (*Broker, error) {
	brokers.mutex.Lock()
	defer brokers.mutex.Unlock()

	if broker, ok := brokers.brokers[nodeID]; ok {
		return broker, nil
	}

	if brokerInfo, ok := brokers.brokersInfo[nodeID]; ok {
		broker, err := NewBroker(fmt.Sprintf("%s:%d", brokerInfo.Host, brokerInfo.Port), brokerInfo.NodeID, brokers.config)
		if err == nil {
			brokers.brokers[nodeID] = broker
			return broker, nil
		} else {
			return nil, fmt.Errorf("could not init broker for node[%d](%s:%d):%s", nodeID, brokerInfo.Host, brokerInfo.Port, err)
		}
	} else {
		return nil, fmt.Errorf("could not get broker info with nodeID[%d]", nodeID)
	}
}

func (brokers *Brokers) RequestMetaData(clientID string, topics []string) (r MetadataResponse, err error) {
	for _, brokerInfo := range brokers.brokersInfo {
		broker, err := brokers.GetBroker(brokerInfo.NodeID)
		if err != nil {
			glog.Infof("get broker from %s:%d error: %s", brokerInfo.Host, brokerInfo.Port, err)
			continue
		}
		r, err = broker.requestMetaData(clientID, topics)

		if err == nil {
			return r, nil
		}

		glog.Errorf("get metadata of %v from %s error: %s", topics, broker.address, err)
		if e, ok := err.(*Error); ok {
			return r, e
		}
		time.Sleep(time.Millisecond * 200)
	}

	return r, &noAvaliableBrokers
}

// RequestOffsets return the offset values array. return all partitions if partitionID < 0
func (brokers *Brokers) RequestOffsets(clientID, topic string, partitionID int32, timeValue int64, offsets uint32) ([]OffsetsResponse, error) {
	// have to find which leader own the partition by request metadata
	// TODO cache
	metadataResponse, err := brokers.RequestMetaData(clientID, []string{topic})
	if err != nil {
		return nil, fmt.Errorf("could not get metadata of topic[%s]:%s", topic, err)
	}

	// TODO only one topic
	topicMetadata := metadataResponse.TopicMetadatas[0]

	if partitionID >= 0 {
		for _, x := range topicMetadata.PartitionMetadatas {
			if partitionID == x.PartitionID {
				if leader, err := brokers.GetBroker(x.Leader); err != nil {
					return nil, fmt.Errorf("could not find leader of %s[%d]:%s", topic, partitionID, err)
				} else {
					offsetsResponse, err := leader.requestOffsets(clientID, topic, []int32{partitionID}, timeValue, offsets)
					if err != nil {
						return nil, err
					} else {
						return []OffsetsResponse{offsetsResponse}, nil
					}
				}
			}
		}
		return nil, fmt.Errorf("could not find partition %d in topic %s", partitionID, topic)
	} else {
		// try to get all partition offsets
		offsetsRequestsMapping := make(map[int32][]int32, 0) //nodeID: partitionIDs
		for _, x := range topicMetadata.PartitionMetadatas {
			if _, ok := offsetsRequestsMapping[x.Leader]; ok {
				offsetsRequestsMapping[x.Leader] = append(offsetsRequestsMapping[x.Leader], x.PartitionID)
			} else {
				offsetsRequestsMapping[x.Leader] = []int32{x.PartitionID}
			}
		}

		rst := make([]OffsetsResponse, 0)
		for leaderID, partitionIDs := range offsetsRequestsMapping {
			if leader, err := brokers.GetBroker(leaderID); err != nil {
				return nil, fmt.Errorf("could not find leader of %s[%v]:%s", topic, partitionIDs, err)
			} else {
				offsetsResponse, err := leader.requestOffsets(clientID, topic, partitionIDs, timeValue, offsets)
				if err != nil {
					// TODO display error for the partition and go on?
					return nil, err
				}
				rst = append(rst, offsetsResponse)
			}
		}
		return rst, nil
	}
}
func (brokers *Brokers) findLeader(clientID, topic string, partitionID int32) (int32, error) {
	metadataResponse, err := brokers.RequestMetaData(clientID, []string{topic})
	if err != nil {
		return -1, fmt.Errorf("could not get metadata of topic %s: %s", topic, err)
	}

	partitionMetadatas := metadataResponse.TopicMetadatas[0].PartitionMetadatas
	for _, partitionMetadata := range partitionMetadatas {
		if partitionMetadata.PartitionID == partitionID {
			return partitionMetadata.Leader, nil
		}
	}
	return -1, fmt.Errorf("could not find out leader of topic %s", topic)
}

func (brokers *Brokers) FindCoordinator(clientID, groupID string) (r FindCoordinatorResponse, err error) {
	var broker *Broker
	for _, brokerInfo := range brokers.brokersInfo {
		broker, err = brokers.GetBroker(brokerInfo.NodeID)
		if err != nil {
			glog.Errorf("get broker[%d] error:%s", brokerInfo.NodeID, err)
			continue
		}
		r, err = broker.findCoordinator(clientID, groupID)
		if err != nil {
			glog.Errorf("could not find coordinator from %s:%s", broker.address, err)
		} else {
			return
		}
	}

	return FindCoordinatorResponse{}, fmt.Errorf("could not find coordinator from all brokers")
}

// ListPartitionReassignments requests ListPartitionReassignments from controller and returns response
func (brokers *Brokers) ListPartitionReassignments(req ListPartitionReassignmentsRequest) (r ListPartitionReassignmentsResponse, err error) {
	controller, err := brokers.GetBroker(brokers.Controller())
	if err != nil {
		return r, fmt.Errorf("could not create controller broker: %s", err)
	}
	resp, err := controller.RequestAndGet(req)
	if err != nil {
		return r, fmt.Errorf("could not get ListPartitionReassignments response from controller: %s", err)
	}
	return resp.(ListPartitionReassignmentsResponse), nil
}

// Request try to do request from all brokers until get the response
func (brokers *Brokers) Request(req Request) (Response, error) {
	for _, brokerInfo := range brokers.brokersInfo {
		broker, err := brokers.GetBroker(brokerInfo.NodeID)
		if err != nil {
			continue
		}
		resp, err := broker.RequestAndGet(req)
		if err != nil {
			glog.Infof("request %d from %s error: %s", req.API(), broker.address, err)
			continue
		} else {
			return resp, nil
		}
	}

	return nil, fmt.Errorf("could not request %d from all brokers", req.API())
}
