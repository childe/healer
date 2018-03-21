package healer

import (
	"errors"
	"time"

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

	err = p.refreshTopicMeta()
	if err != nil {
		glog.Error(err)
		return nil
	}

	go func() {
		for range time.NewTicker(time.Duration(config.MetadataMaxAgeMS) * time.Millisecond).C {
			err := p.refreshTopicMeta()
			if err != nil {
				glog.Error(err)
			}
		}
	}()

	return p
}

func (p *Producer) refreshTopicMeta() error {
	for i := 0; i < p.config.FetchTopicMetaDataRetrys; i++ {
		metadataResponse, err := p.brokers.RequestMetaData(p.config.ClientID, []string{p.topic})
		if err != nil {
			glog.Errorf("get topic metadata error: %s", err)
			continue
		}
		if len(metadataResponse.TopicMetadatas) == 0 {
			glog.Errorf("get topic metadata error: %s", zeroTopicMetadata)
			continue
		}
		p.topicMeta = metadataResponse.TopicMetadatas[0]
		return nil
	}
	return errors.New("failed to get topic meta after all tries")
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
