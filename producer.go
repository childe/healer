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
}

func NewProducer(topic string, config *ProducerConfig) *Producer {
	err := config.checkValid()
	if er != nil {
		glog.Errorf("producer config error: %s", err)
		return nil
	}
	brokers, err := NewBrokers(config["bootstrap.servers"].(string), clientID, connectTimeout, timeout)
	if err != nil {
		glog.Errorf("[%s] init brokers error:%s", topic, err)
		return nil
	}

	p = &Producer{
		config: config,
		topic:  topic,
	}

	return p
}

func (p *Producer) AddMessage(key []byte, value []byte) error {
	if key == nil || len(key) == 0 {
		return p.currentProducer.AddMessage(key, value)
	}
	partitionID := int32(murmur.MurmurHash2(key, 0)) % count
	if s, ok := p.simpleProducers[partitionID]; ok {
		return s.AddMessage(key, value)
	} else {
		simpleProducer := NewSimpleProducer(p.topic, partitionID, p.config)
		p.simpleProducers[partitionID] = simpleProducer
		return simpleProducer.AddMessage(key, value)
	}
}
