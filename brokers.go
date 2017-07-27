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
	metadataResponse, err := broker.requestMetaData(&topic)
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
		metadataResponse, err := broker.requestMetaData(topic)
		if err != nil {
			glog.Infof("could not get metadata from %s:%s", broker.address, err)
		} else {
			return metadataResponse, nil
		}
	}

	return nil, fmt.Errorf("could not get metadata from all brokers")
}

// RequestOffsets return the offset values array. return all partitions if partitionID < 0
func (brokers *Brokers) RequestOffsets(topic string, partitionID int32, timeValue int64, offsets uint32) ([]*OffsetsResponse, error) {
	// have to find which leader own the partition by request metadata
	// TODO cache
	metadataResponse, err := brokers.RequestMetaData(&topic)
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
				if leader, ok := brokers.brokers[x.Leader]; !ok {
					return nil, fmt.Errorf("could not find leader of %s[%d]", topic, partitionID)
				} else {
					offsetsResponse, err := leader.requestOffsets(topic, []uint32{uID}, timeValue, offsets)
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
			if leader, ok := brokers.brokers[leaderID]; !ok {
				return nil, fmt.Errorf("could not find leader of %s[%v]", topic, partitionIDs)
			} else {
				offsetsResponse, err := leader.requestOffsets(topic, partitionIDs, timeValue, offsets)
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
