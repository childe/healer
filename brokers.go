//TODO referesh metadata when running into error

package healer

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/golang/glog"
)

type Brokers struct {
	config           *BrokerConfig
	bootstrapServers string

	brokersInfo map[int32]*BrokerInfo
	brokers     map[int32]*Broker
}

// get all brokers meda info from MetaData api
// it DON'T create connection to each broker
func newBrokersFromOne(broker *Broker, clientID string, config *BrokerConfig) (*Brokers, error) {
	brokers := &Brokers{
		config:      config,
		brokersInfo: make(map[int32]*BrokerInfo),
		brokers:     make(map[int32]*Broker),
	}

	// TODO set topics to [""] ?
	metadataResponse, err := broker.requestMetaData(clientID, []string{""})
	if metadataResponse == nil || metadataResponse.Brokers == nil {
		return nil, err
	}

	for _, brokerInfo := range metadataResponse.Brokers {
		brokers.brokersInfo[brokerInfo.NodeId] = brokerInfo
		if broker.GetAddress() == fmt.Sprintf("%s:%d", brokerInfo.Host, brokerInfo.Port) {
			brokers.brokers[brokerInfo.NodeId] = broker
		}
	}

	glog.Infof("got %d brokers", len(brokers.brokersInfo))
	if glog.V(2) {
		for nodeID, broker := range brokers.brokersInfo {
			glog.Infof("%d %s:%d", nodeID, broker.Host, broker.Port)
		}
	}

	return brokers, nil
}

func NewBrokers(bootstrapServers string, clientID string, config *BrokerConfig) (*Brokers, error) {
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
					for range time.NewTicker(time.Duration(config.MetadataRefreshIntervalMS) * time.Millisecond).C {
						if !brokers.refreshMetadata() {
							glog.Error("refresh metadata error")
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

func (brokers *Brokers) refreshMetadata() bool {
	topics := []string{""}
	clientID := "healer-refresh-metadata"

	// from origianl bootstrapServers
	for _, brokerAddr := range strings.Split(brokers.bootstrapServers, ",") {
		broker, err := NewBroker(brokerAddr, -1, brokers.config)
		if err != nil {
			glog.Errorf("create broker[%s] error: %s", brokerAddr, err)
			continue
		}

		metadataResponse, err := broker.requestMetaData(clientID, topics)
		if metadataResponse == nil || metadataResponse.Brokers == nil {
			glog.Errorf("request metadata error: %s", err)
			continue
		}

		brokersInfo := make(map[int32]*BrokerInfo)
		for _, brokerInfo := range metadataResponse.Brokers {
			brokersInfo[brokerInfo.NodeId] = brokerInfo
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
		if metadataResponse == nil || metadataResponse.Brokers == nil {
			glog.Errorf("request metadata error: %s", err)
			continue
		}

		brokersInfo := make(map[int32]*BrokerInfo)
		for _, brokerInfo := range metadataResponse.Brokers {
			brokersInfo[brokerInfo.NodeId] = brokerInfo
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
// TODO retry to get metadata if `could not get broker info from nodeID`
func (brokers *Brokers) NewBroker(nodeID int32) (*Broker, error) {
	if nodeID == -1 {
		for nodeID, brokerInfo := range brokers.brokersInfo {
			broker, err := NewBroker(fmt.Sprintf("%s:%d", brokerInfo.Host, brokerInfo.Port), nodeID, brokers.config)
			if err == nil {
				return broker, nil
			}
		}
		return nil, fmt.Errorf("could not get broker from nodeID[%d]", nodeID)
	}

	if brokerInfo, ok := brokers.brokersInfo[nodeID]; ok {
		broker, err := NewBroker(fmt.Sprintf("%s:%d", brokerInfo.Host, brokerInfo.Port), brokerInfo.NodeId, brokers.config)
		if err == nil {
			return broker, nil
		} else {
			return nil, fmt.Errorf("could not init broker for node[%d](%s:%d):%s", nodeID, brokerInfo.Host, brokerInfo.Port, err)
		}
	} else {
		return nil, fmt.Errorf("could not get broker info with nodeID[%d]", nodeID)
	}
}

// GetBroke returns random one broker if nodeID is -1
func (brokers *Brokers) GetBroker(nodeID int32) (*Broker, error) {
	if nodeID == -1 {
		for _, broker := range brokers.brokers {
			if broker.dead == false {
				return broker, nil
			}
		}
		for nodeID, brokerInfo := range brokers.brokersInfo {
			broker, err := NewBroker(fmt.Sprintf("%s:%d", brokerInfo.Host, brokerInfo.Port), nodeID, brokers.config)
			if err == nil {
				brokers.brokers[nodeID] = broker
				return broker, nil
			}
		}
		return nil, fmt.Errorf("could not get broker from nodeID[%d]", nodeID)
	}

	if broker, ok := brokers.brokers[nodeID]; ok {
		if broker.dead == false {
			return broker, nil
		}
	}

	if brokerInfo, ok := brokers.brokersInfo[nodeID]; ok {
		broker, err := NewBroker(fmt.Sprintf("%s:%d", brokerInfo.Host, brokerInfo.Port), brokerInfo.NodeId, brokers.config)
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

func (brokers *Brokers) RequestMetaData(clientID string, topics []string) (*MetadataResponse, error) {
	for _, brokerInfo := range brokers.brokersInfo {
		broker, err := brokers.GetBroker(brokerInfo.NodeId)
		if err != nil {
			glog.Infof("get broker from %s:%d error: %s", brokerInfo.Host, brokerInfo.Port, err)
		}
		metadataResponse, err := broker.requestMetaData(clientID, topics)

		if err == nil {
			return metadataResponse, err
		}

		glog.Errorf("get metadata from %s error: %s", broker.address, err)
		if "*healer.Error" == reflect.TypeOf(err).String() && !err.(*Error).Retriable {
			glog.Info("error not retriable")
			return metadataResponse, err
		}
	}

	return nil, fmt.Errorf("could not get metadata from all brokers")
}

// RequestOffsets return the offset values array. return all partitions if partitionID < 0
func (brokers *Brokers) RequestOffsets(clientID, topic string, partitionID int32, timeValue int64, offsets uint32) ([]*OffsetsResponse, error) {
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
						return []*OffsetsResponse{offsetsResponse}, nil
					}
				}
			}
		}
		return nil, fmt.Errorf("could not find partition %d in topic %s", partitionID, topic)
	}

	// try to get all partition offsets
	offsetsRequestsMapping := make(map[int32][]int32, 0) //nodeID: partitionIDs
	for _, x := range topicMetadata.PartitionMetadatas {
		if _, ok := offsetsRequestsMapping[x.Leader]; ok {
			offsetsRequestsMapping[x.Leader] = append(offsetsRequestsMapping[x.Leader], x.PartitionID)
		} else {
			offsetsRequestsMapping[x.Leader] = []int32{x.PartitionID}
		}
	}

	rst := make([]*OffsetsResponse, 0)
	if partitionID < 0 {
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
	return nil, nil
}
func (brokers *Brokers) findLeader(clientID, topic string, partitionID int32) (int32, error) {
	metadataResponse, err := brokers.RequestMetaData(clientID, []string{topic})
	if err != nil {
		return -1, fmt.Errorf("could not get metadata of topic %s:%s", topic, err)
	}

	partitionMetadatas := metadataResponse.TopicMetadatas[0].PartitionMetadatas
	for _, partitionMetadata := range partitionMetadatas {
		if partitionMetadata.PartitionID == partitionID {
			return partitionMetadata.Leader, nil
		}
	}
	return -1, fmt.Errorf("could not find out leader of topic %s", topic)
}

//func (brokers *Brokers) RequestListGroups(clientID string) (*ListGroupsResponse, error) {
//for _, brokerInfo := range brokers.brokersInfo {
//broker, err := brokers.GetBroker(brokerInfo.NodeId)
//if err != nil {
//continue
//}
//response, err := broker.requestListGroups(clientID)
//if err != nil {
//glog.Infof("could not get metadata from %s:%s", broker.address, err)
//} else {
//return response, nil
//}
//}

//return nil, fmt.Errorf("could not list groups from all brokers")
//}

func (brokers *Brokers) FindCoordinator(clientID, groupID string) (*FindCoordinatorResponse, error) {
	for _, brokerInfo := range brokers.brokersInfo {
		broker, err := brokers.GetBroker(brokerInfo.NodeId)
		if err != nil {
			glog.Errorf("get broker[%d] error:%s", brokerInfo.NodeId, err)
			continue
		}
		response, err := broker.findCoordinator(clientID, groupID)
		if err != nil {
			glog.Errorf("could not find coordinator from %s:%s", broker.address, err)
		} else {
			return response, nil
		}
	}

	return nil, fmt.Errorf("could not find coordinator from all brokers")
}

func (brokers *Brokers) RequestDescribeGroups(clientID string, groups []string) (*DescribeGroupsResponse, error) {
	for _, brokerInfo := range brokers.brokersInfo {
		broker, err := brokers.GetBroker(brokerInfo.NodeId)
		if err != nil {
			continue
		}
		response, err := broker.requestDescribeGroups(clientID, groups)
		if err != nil {
			glog.Infof("post describe groups request from %s error:%s", broker.address, err)
		} else {
			return response, nil
		}
	}

	return nil, fmt.Errorf("could not describe groups from all brokers")
}

func (brokers *Brokers) Request(req Request) ([]byte, error) {
	for _, brokerInfo := range brokers.brokersInfo {
		broker, err := brokers.GetBroker(brokerInfo.NodeId)
		if err != nil {
			continue
		}
		response, err := broker.Request(req)
		if err != nil {
			glog.Infof("post request[%d] from %s error:%s", req.API(), broker.address, err)
		} else {
			return response, nil
		}
	}

	return nil, fmt.Errorf("could not request %d from all brokers", req.API())
}

func (brokers *Brokers) Close() {
	for _, broker := range brokers.brokers {
		broker.Close()
	}
}
