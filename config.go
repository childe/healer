package healer

import "errors"

type ProducerConfig struct {
	BootstrapServers         string
	ClientID                 string
	Acks                     int16
	CompressionType          string
	BatchSize                int
	MessageMaxCount          int
	FlushIntervalMS          int
	MetadataMaxAgeMS         int
	FetchTopicMetaDataRetrys int

	// TODO
	ConnectionsMaxIdleMS int
	Retries              int
	RequestTimeoutMS     int32
}

var DefaultProducerConfig *ProducerConfig = &ProducerConfig{
	ClientID:                 "healer",
	Acks:                     1,
	CompressionType:          "none",
	BatchSize:                16384,
	MessageMaxCount:          1024,
	FlushIntervalMS:          200,
	MetadataMaxAgeMS:         300000,
	FetchTopicMetaDataRetrys: 3,
	ConnectionsMaxIdleMS:     540000,

	Retries:          0,
	RequestTimeoutMS: 30000,
}

var (
	messageMaxCountError   = errors.New("message.max.count must > 0")
	flushIntervalMSError   = errors.New("flush.interval.ms must > 0")
	unknownCompressionType = errors.New("unknown compression type")
	bootstrapServersNotSet = errors.New("bootstrap servers not set")
)

func (config *ProducerConfig) checkValid() error {
	if config.BootstrapServers == "" {
		return bootstrapServersNotSet
	}
	if config.MessageMaxCount <= 0 {
		return messageMaxCountError
	}
	if config.FlushIntervalMS <= 0 {
		return flushIntervalMSError
	}

	switch config.CompressionType {
	case "none":
	case "gzip":
	case "snappy":
	case "lz4":
	default:
		return unknownCompressionType
	}
	return nil
}

type BrokerConfig struct {
	ConnectTimeoutMS    int
	TimeoutMS           int
	TimeoutMSForEachAPI []int
}

func DefaultBrokerConfig() *BrokerConfig {
	return &BrokerConfig{
		ConnectTimeoutMS:    60000,
		TimeoutMS:           30000,
		TimeoutMSForEachAPI: make([]int, 0),
	}
}

var (
	brokerAddressNotSet = errors.New("broker address not set in broker config")
)

func (c *BrokerConfig) checkValid() error {
	return nil
}
