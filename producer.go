package healer

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/aviddiviner/go-murmur"
)

type ctxKey string

var leaderKey ctxKey = "leader"
var parentProducerKey ctxKey = "parentProducer"

type Producer struct {
	config ProducerConfig
	topic  string

	brokers   *Brokers
	topicMeta TopicMetadata

	pidToSimpleProducers map[int32]*SimpleProducer
	leaderBrokersMapping map[int32]*Broker
	currentProducer      *SimpleProducer
	lock                 sync.Mutex

	ctx context.Context
}

// NewProducer creates a new console producer.
// config can be a map[string]interface{} or a ProducerConfig,
// use DefaultProducerConfig if config is nil
func NewProducer(topic string, config interface{}) (*Producer, error) {
	cfg, err := createProducerConfig(config)
	logger.Info("create producer", "origin_config", config, "final_config", cfg)
	if err != nil {
		return nil, err
	}

	p := &Producer{
		config:               cfg,
		topic:                topic,
		pidToSimpleProducers: make(map[int32]*SimpleProducer),
		leaderBrokersMapping: make(map[int32]*Broker),
	}

	brokerConfig := getBrokerConfigFromProducerConfig(&cfg)
	p.brokers, err = NewBrokersWithConfig(cfg.BootstrapServers, brokerConfig)
	if err != nil {
		err = fmt.Errorf("init brokers error: %w", err)
		return nil, err
	}

	err = p.updateCurrentSimpleProducer()
	if err != nil {
		err = fmt.Errorf("update current simple consumer of %s error: %w", p.topic, err)
		return nil, err
	}

	ctx := context.Background()
	p.ctx = context.WithValue(ctx, parentProducerKey, p)

	go func() {
		for range time.NewTicker(time.Duration(cfg.MetadataMaxAgeMS) * time.Millisecond).C {
			err := p.updateCurrentSimpleProducer()
			if err != nil {
				logger.Error(err, "refresh simple producer failed", "topic", p.topic)
			}
		}
	}()

	return p, nil
}

// update metadata and currentProducer
func (p *Producer) updateCurrentSimpleProducer() error {
	metadataResponse, err := p.brokers.RequestMetaData(p.config.ClientID, []string{p.topic})
	if err == nil {
		err = metadataResponse.Error()
	}
	if err != nil {
		return fmt.Errorf("get metadata of %s error: %w", p.topic, err)
	}
	p.topicMeta = metadataResponse.TopicMetadatas[0]

	validPartitionID := make([]int32, 0)
	for _, partition := range p.topicMeta.PartitionMetadatas {
		if partition.PartitionErrorCode == 0 {
			validPartitionID = append(validPartitionID, partition.PartitionID)
		}
	}
	partitionID := validPartitionID[rand.Int31n(int32(len(validPartitionID)))]
	sp, err := NewSimpleProducer(context.Background(), p.topic, partitionID, p.config)
	if err != nil {
		return fmt.Errorf("change current simple producer to %s-%d error: %w", p.topic, partitionID, err)
	}

	p.lock.Lock()
	defer p.lock.Unlock()
	if p.currentProducer == nil {
		logger.Info("init current simple producer", "leader", sp.leader)
		p.currentProducer = sp
	} else {
		older := p.currentProducer
		logger.Info("update current simple producer", "older", older, "oldLeader", older.leader, "new", sp, "newLeader", sp.leader)
		p.currentProducer = sp
		older.Close()
	}

	return nil
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
		p.lock.Lock()
		defer p.lock.Unlock()
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

	ctx := context.WithValue(p.ctx, leaderKey, broker)
	sp, err := NewSimpleProducer(ctx, p.topic, partitionID, p.config)
	if err != nil {
		return nil, fmt.Errorf("could not create simple producer from the %s-%d", p.topic, partitionID)
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
			logger.V(1).Info("simple producer closed, retry", "producer", simpleProducer)
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
		sp.Close()
	}

	for _, broker := range p.leaderBrokersMapping {
		broker.Close()
	}
}
