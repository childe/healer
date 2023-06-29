package healer

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/aviddiviner/go-murmur"
	"github.com/golang/glog"
)

type Producer struct {
	config *ProducerConfig
	topic  string

	brokers   *Brokers
	topicMeta TopicMetadata

	pidToSimpleProducers map[int32]*SimpleProducer
	leaderBrokersMapping map[int32]*Broker
	currentProducer      *SimpleProducer
}

// NewProducer creates a new console producer
func NewProducer(topic string, config *ProducerConfig) *Producer {
	var err error
	err = config.checkValid()
	if err != nil {
		glog.Errorf("producer config error: %s", err)
		return nil
	}

	p := &Producer{
		config:               config,
		topic:                topic,
		pidToSimpleProducers: make(map[int32]*SimpleProducer),
		leaderBrokersMapping: make(map[int32]*Broker),
	}

	brokerConfig := getBrokerConfigFromProducerConfig(config)
	p.brokers, err = NewBrokersWithConfig(config.BootstrapServers, brokerConfig)
	if err != nil {
		glog.Errorf("init brokers error: %s", err)
		return nil
	}

	err = p.changeCurrentSimpleConsumer()
	if err != nil {
		glog.Errorf("get metadata of topic %s error: %v", p.topic, err)
		return nil
	}

	go func() {
		for range time.NewTicker(time.Duration(config.MetadataMaxAgeMS) * time.Millisecond).C {
			err := p.changeCurrentSimpleConsumer()
			if err != nil {
				glog.Errorf("refresh metadata error in producer %s ticker: %v", p.topic, err)
			}
		}
	}()

	return p
}

// update metadata and currentProducer
func (p *Producer) changeCurrentSimpleConsumer() error {
	for i := 0; i < p.config.FetchTopicMetaDataRetrys; i++ {
		metadataResponse, err := p.brokers.RequestMetaData(p.config.ClientID, []string{p.topic})
		if err == nil {
			err = metadataResponse.Error()
		}
		if err != nil {
			glog.Errorf("get metadata of %s error: %s", p.topic, err)
			continue
		}
		p.topicMeta = metadataResponse.TopicMetadatas[0]

		rand.Seed(time.Now().UnixNano())
		validPartitionID := make([]int32, 0)
		for _, partition := range p.topicMeta.PartitionMetadatas {
			if partition.PartitionErrorCode == 0 {
				validPartitionID = append(validPartitionID, partition.PartitionID)
			}
		}
		partitionID := validPartitionID[rand.Int31n(int32(len(validPartitionID)))]
		sp := NewSimpleProducer(p.topic, partitionID, p.config, nil)
		if sp == nil {
			glog.Errorf("could not update current simple producer from the %s-%d. use the previous one", p.topic, partitionID)
			return nil
		}
		if p.currentProducer == nil {
			glog.Infof("update current simple producer to %s", sp.leader.GetAddress())
		} else {
			glog.Infof("update current simple producer from the %s to %s", p.currentProducer.leader.GetAddress(), sp.leader.GetAddress())
			p.currentProducer.Close()
		}
		p.currentProducer = sp

		return nil
	}
	return fmt.Errorf("failed to get topic meta of %s after %d tries", p.topic, p.config.FetchTopicMetaDataRetrys)
}

// getLeaderID return the leader broker id of the partition from metadata cache
func (p *Producer) getLeaderID(pid int32) (int32, error) {
	for _, partition := range p.topicMeta.PartitionMetadatas {
		if partition.PartitionID == pid {
			if partition.PartitionErrorCode == 0 {
				return partition.Leader, nil
			}
			return -1, getErrorFromErrorCode(partition.PartitionErrorCode)
		}
	}
	return -1, fmt.Errorf("partition %s-%d not found in metadata", p.topic, pid)
}

// getSimpleProducer return the simple producer. if key is nil, return the current simple producer
func (p *Producer) getSimpleProducer(key []byte) (*SimpleProducer, error) {
	if key == nil {
		return p.currentProducer, nil
	}

	partitionID := int32(murmur.MurmurHash2(key, 0) % uint32(len(p.topicMeta.PartitionMetadatas)))

	if sp, ok := p.pidToSimpleProducers[partitionID]; ok {
		return sp, nil
	}

	leaderID, err := p.getLeaderID(partitionID)
	if err != nil {
		return nil, err
	}
	broker, ok := p.leaderBrokersMapping[leaderID]
	if !ok {
		broker, err = p.brokers.NewBroker(leaderID)
		if err != nil {
			return nil, fmt.Errorf("create broker %d error: %s", leaderID, err)
		}
		p.leaderBrokersMapping[leaderID] = broker
	}
	sp := NewSimpleProducer(p.topic, partitionID, p.config, broker)
	if sp == nil {
		return nil, fmt.Errorf("could not create simple producer from the %s-%d.", p.topic, partitionID)
	}
	p.pidToSimpleProducers[partitionID] = sp
	return sp, nil
}

// AddMessage add message to the producer, if key is nil, use current simple producer, else use the simple producer of the partition of the key
// if the simple producer of the partition of the key not exist, create a new one
// if the simple producer closed, retry 3 times
func (p *Producer) AddMessage(key []byte, value []byte) error {
	for i := 0; i < 3; i++ {
		simpleProducer, err := p.getSimpleProducer(key)
		if err != nil {
			return err
		}
		err = simpleProducer.AddMessage(key, value)
		if err == ErrProducerClosed { // maybe current simple-producer closed in ticker, retry
			continue
		}
		return err
	}
	return nil
}

// Close close all simple producers in the console producer
func (p *Producer) Close() {
	if p.currentProducer != nil {
		p.currentProducer.Close()
	}
	for _, sp := range p.pidToSimpleProducers {
		if p.currentProducer != sp {
			sp.Close()
		}
	}
}
