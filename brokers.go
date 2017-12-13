package healer

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/golang/glog"
)

type Brokers struct {
	brokersInfo   map[int32]*BrokerInfo
	brokers       map[int32]*Broker
	connecTimeout int
	timeout       int
}

func newBrokersFromOne(broker *Broker, clientID string, connecTimeout int, timeout int) (*Brokers, error) {
	brokers := &Brokers{
		connecTimeout: connecTimeout,
		timeout:       timeout,
		brokersInfo:   make(map[int32]*BrokerInfo),
		brokers:       make(map[int32]*Broker),
	}

	topic := ""
	metadataResponse, err := broker.requestMetaData(clientID, &topic)
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
		brokers.brokersInfo[brokerInfo.NodeId] = brokerInfo
		if broker.address == fmt.Sprintf("%s:%d", brokerInfo.Host, brokerInfo.Port) {
			broker.nodeID = brokerInfo.NodeId
			brokers.brokers[brokerInfo.NodeId] = broker
		}
	}

	glog.Infof("got %d brokers", len(brokers.brokersInfo))

	return brokers, nil
}

func NewBrokers(brokerList string, clientID string, connecTimeout int, timeout int) (*Brokers, error) {
	for _, brokerAddr := range strings.Split(brokerList, ",") {
		broker, err := NewBroker(brokerAddr, -1, connecTimeout, timeout)
		// TODO conn not established?
		if err != nil {
			glog.Infof("init broker from %s error:%s", brokerAddr, err)
		} else {
			defer broker.conn.Close()

			brokers, err := newBrokersFromOne(broker, clientID, connecTimeout, timeout)
			if err != nil {
				glog.Infof("could not get broker list from %s:%s", broker.address, err)
			} else {
				return brokers, nil
			}
		}
	}
	return nil, fmt.Errorf("could not get any available broker from %s", brokerList)

}

// GetBroke returns random one broker if nodeID is -1
func (brokers *Brokers) GetBroker(nodeID int32) (*Broker, error) {
	if nodeID == -1 {
		for _, broker := range brokers.brokers {
			return broker, nil
		}
		for nodeID, brokerInfo := range brokers.brokersInfo {
			broker, err := NewBroker(fmt.Sprintf("%s:%d", brokerInfo.Host, brokerInfo.Port), nodeID, brokers.connecTimeout, brokers.timeout)
			if err == nil {
				return broker, nil
			}
		}
		return nil, fmt.Errorf("could not get broker from nodeID[%d]", nodeID)
	}

	if broker, ok := brokers.brokers[nodeID]; ok {
		return broker, nil
	}

	if brokerInfo, ok := brokers.brokersInfo[nodeID]; ok {
		return NewBroker(fmt.Sprintf("%s:%d", brokerInfo.Host, brokerInfo.Port), brokerInfo.NodeId, brokers.connecTimeout, brokers.timeout)
	} else {
		return nil, fmt.Errorf("could not get broker info with nodeID[%d]", nodeID)
	}
}

func (brokers *Brokers) RequestMetaData(clientID string, topic *string) (*MetadataResponse, error) {
	for _, brokerInfo := range brokers.brokersInfo {
		broker, err := brokers.GetBroker(brokerInfo.NodeId)
		if err != nil {
			glog.Infof("could not get metadata from %s:%d:%s", brokerInfo.Host, brokerInfo.Port, err)
			continue
		}
		metadataResponse, err := broker.requestMetaData(clientID, topic)
		if err != nil {
			glog.Infof("could not get metadata from %s:%s", broker.address, err)
		} else {
			return metadataResponse, nil
		}
	}

	return nil, fmt.Errorf("could not get metadata from all brokers")
}

// RequestOffsets return the offset values array. return all partitions if partitionID < 0
func (brokers *Brokers) RequestOffsets(clientID, topic string, partitionID int32, timeValue int64, offsets uint32) ([]*OffsetsResponse, error) {
	// have to find which leader own the partition by request metadata
	// TODO cache
	metadataResponse, err := brokers.RequestMetaData(clientID, &topic)
	if err != nil {
		return nil, fmt.Errorf("could not get metadata of topic[%s]:%s", topic, err)
	}

	// TODO only one topic
	topicMetadata := metadataResponse.TopicMetadatas[0]

	if topicMetadata.TopicErrorCode != 0 {
		return nil, fmt.Errorf("could not get metadata of topic[%s]:%s", topic, AllError[topicMetadata.TopicErrorCode].ErrorMsg)
	}

	if partitionID >= 0 {
		uID := uint32(partitionID)
		for _, x := range topicMetadata.PartitionMetadatas {
			if uID == x.PartitionId {
				if leader, err := brokers.GetBroker(x.Leader); err != nil {
					return nil, fmt.Errorf("could not find leader of %s[%d]:%s", topic, partitionID, err)
				} else {
					offsetsResponse, err := leader.requestOffsets(clientID, topic, []uint32{uID}, timeValue, offsets)
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
	offsetsRequestsMapping := make(map[int32][]uint32, 0) //nodeID: partitionIDs
	for _, x := range topicMetadata.PartitionMetadatas {
		if _, ok := offsetsRequestsMapping[x.Leader]; ok {
			offsetsRequestsMapping[x.Leader] = append(offsetsRequestsMapping[x.Leader], x.PartitionId)
		} else {
			offsetsRequestsMapping[x.Leader] = []uint32{x.PartitionId}
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
	metadataResponse, err := brokers.RequestMetaData(clientID, &topic)
	if err != nil {
		return -1, fmt.Errorf("could not get metadata of topic %s:%s", topic, err)
	}

	partitionMetadatas := metadataResponse.TopicMetadatas[0].PartitionMetadatas
	for _, partitionMetadata := range partitionMetadatas {
		if int32(partitionMetadata.PartitionId) == partitionID {
			return partitionMetadata.Leader, nil
		}
	}
	return -1, fmt.Errorf("could not find out leader of topic %s", topic)
}

func (brokers *Brokers) RequestListGroups(clientID string) (*ListGroupsResponse, error) {
	for _, brokerInfo := range brokers.brokersInfo {
		broker, err := brokers.GetBroker(brokerInfo.NodeId)
		if err != nil {
			continue
		}
		response, err := broker.requestListGroups(clientID)
		if err != nil {
			glog.Infof("could not get metadata from %s:%s", broker.address, err)
		} else {
			return response, nil
		}
	}

	return nil, fmt.Errorf("could not list groups from all brokers")
}

func (brokers *Brokers) FindCoordinator(clientID, groupID string) (*FindCoordinatorResponse, error) {
	for _, brokerInfo := range brokers.brokersInfo {
		broker, err := brokers.GetBroker(brokerInfo.NodeId)
		if err != nil {
			continue
		}
		response, err := broker.findCoordinator(clientID, groupID)
		if err != nil {
			glog.Infof("could not find coordinator from %s:%s", broker.address, err)
		} else {
			return response, nil
		}
	}

	return nil, fmt.Errorf("could not list groups from all brokers")
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
