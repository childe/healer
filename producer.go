package healer

import (
	"github.com/aviddiviner/go-murmur"
	"github.com/golang/glog"
)

type Producer struct {
	config          *ProducerConfig
	topic           string
	simpleProducers map[int32]*SimpleProducer
	currentProducer *SimpleProducer
	brokers         *Brokers
	topicMeta       *TopicMetadata
}

func NewProducer(topic string, config *ProducerConfig) *Producer {
	var err error
	err = config.checkValid()
	if err != nil {
		glog.Errorf("producer config error: %s", err)
		return nil
	}

	p := &Producer{
		config: config,
		topic:  topic,
	}

	connectTimeout := 30000
	timeout := 60000
	p.brokers, err = NewBrokers(config.BootstrapServers, config.ClientID, connectTimeout, timeout)
	if err != nil {
		glog.Errorf("init brokers error: %s", err)
		return nil
	}

	for i := 0; i < config.FetchTopicMetaDataRetrys; i++ {
		err = p.refreshTopicMeta()
		if err != nil {
			glog.Error("get topic meta error: %s", err)
		} else {
			break
		}
	}
	if err != nil {
		glog.Error("failed to topic meta after %d tries", config.FetchTopicMetaDataRetrys)
		return nil
	}

	return p
}

func (p *Producer) refreshTopicMeta() error {
	metadataResponse, err := p.brokers.RequestMetaData(p.config.ClientID, []string{p.topic})
	if err != nil {
		return err
	}
	if len(metadataResponse.TopicMetadatas) == 0 {
		return zeroTopicMetadata
	}

	p.topicMeta = metadataResponse.TopicMetadatas[0]
	return nil
}

func (p *Producer) AddMessage(key []byte, value []byte) error {
	if key == nil || len(key) == 0 {
		return p.currentProducer.AddMessage(key, value)
	}
	partitionID := int32(murmur.MurmurHash2(key, 0)) % int32(len(p.topicMeta.PartitionMetadatas))
	if s, ok := p.simpleProducers[partitionID]; ok {
		return s.AddMessage(key, value)
	} else {
		simpleProducer := NewSimpleProducer(p.topic, partitionID, p.config)
		p.simpleProducers[partitionID] = simpleProducer
		return simpleProducer.AddMessage(key, value)
	}
}
