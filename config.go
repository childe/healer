package healer

import (
	"errors"
	"fmt"

	"github.com/mitchellh/mapstructure"
)

type NetConfig struct {
	ConnectTimeoutMS    int   `json:"connect.timeout.ms,string" mapstructure:"connect.timeout.ms"`
	TimeoutMS           int   `json:"timeout.ms,string" mapstructure:"timeout.ms"`
	TimeoutMSForEachAPI []int `json:"timeout.ms.for.eachapi" mapstructure:"timeout.ms.for.eachapi"`
	KeepAliveMS         int   `json:"keepalive.ms,string" mapstructure:"keepalive.ms"`
}

type TLSConfig struct {
	Cert               string `json:"cert" mapstructure:"cert"`
	Key                string `json:"key" mapstructure:"key"`
	CA                 string `json:"ca" mapstructure:"ca"`
	InsecureSkipVerify bool   `json:"insecure.skip.verify,string" mapstructure:"insecure.skip.verify"`
	ServerName         string `json:"servername" mapstructure:"servername"`
}

type SaslConfig struct {
	Mechanism string `json:"mechanism" mapstructure:"mechanism"`
	User      string `json:"user" mapstructure:"user"`
	Password  string `json:"password" mapstructure:"password"`
}

type BrokerConfig struct {
	Net                       NetConfig  `json:"net" mapstructure:"net"`
	Sasl                      SaslConfig `json:"sasl" mapstructure:"sasl"`
	MetadataRefreshIntervalMS int        `json:"metadata.refresh.interval.ms,string" mapstructure:"metadata.refresh.interval.ms"`
	TLSEnabled                bool       `json:"tls.enabled,string" mapstructure:"tls.enabled"`
	TLS                       *TLSConfig `json:"tls" mapstructure:"tls"`
}

func DefaultBrokerConfig() *BrokerConfig {
	return &BrokerConfig{
		Net: NetConfig{
			ConnectTimeoutMS:    10000,
			TimeoutMS:           30000,
			TimeoutMSForEachAPI: make([]int, 0),
			KeepAliveMS:         7200000,
		},
		MetadataRefreshIntervalMS: 300 * 1000,
		TLSEnabled:                false,
	}
}

func getBrokerConfigFromConsumerConfig(c ConsumerConfig) *BrokerConfig {
	b := DefaultBrokerConfig()
	b.Net = c.Net
	b.TLSEnabled = c.TLSEnabled
	b.TLS = c.TLS
	b.Sasl = c.Sasl

	if c.MetadataRefreshIntervalMS > 0 {
		b.MetadataRefreshIntervalMS = c.MetadataRefreshIntervalMS
	}
	return b
}

func getBrokerConfigFromProducerConfig(p *ProducerConfig) *BrokerConfig {
	b := DefaultBrokerConfig()
	b.Net = p.Net
	b.TLSEnabled = p.TLSEnabled
	b.TLS = p.TLS
	b.Sasl = p.Sasl
	if p.MetadataRefreshIntervalMS > 0 {
		b.MetadataRefreshIntervalMS = p.MetadataRefreshIntervalMS
	}
	return b
}

var (
	errBrokerAddressNotSet = errors.New("broker address not set in broker config")
)

func (c *BrokerConfig) checkValid() error {
	return nil
}

type ConsumerConfig struct {
	Net                  NetConfig  `json:"net" mapstructure:"net"`
	Sasl                 SaslConfig `json:"sasl" mapstructure:"sasl"`
	BootstrapServers     string     `json:"bootstrap.servers" mapstructure:"bootstrap.servers"`
	ClientID             string     `json:"client.id" mapstructure:"client.id"`
	GroupID              string     `json:"group.id" mapstructure:"group.id"`
	RetryBackOffMS       int        `json:"retry.backoff.ms,string" mapstructure:"retry.backoff.ms"`
	MetadataMaxAgeMS     int        `json:"metadata.max.age.ms,string" mapstructure:"metadata.max.age.ms"`
	SessionTimeoutMS     int32      `json:"session.timeout.ms,string" mapstructure:"session.timeout.ms"`
	FetchMaxWaitMS       int32      `json:"fetch.max.wait.ms,string" mapstructure:"fetch.max.wait.ms"`
	FetchMaxBytes        int32      `json:"fetch.max.bytes,string" mapstructure:"fetch.max.bytes"` // if this is too small, healer will double it automatically
	FetchMinBytes        int32      `json:"fetch.min.bytes,string" mapstructure:"fetch.min.bytes"`
	FromBeginning        bool       `json:"from.beginning,string" mapstructure:"from.beginning"`
	AutoCommit           bool       `json:"auto.commit,string" mapstructure:"auto.commit"`
	AutoCommitIntervalMS int        `json:"auto.commit.interval.ms,string" mapstructure:"auto.commit.interval.ms"`
	OffsetsStorage       int        `json:"offsets.storage,string" mapstructure:"offsets.storage"`

	MetadataRefreshIntervalMS int `json:"metadata.refresh.interval.ms,string" mapstructure:"metadata.refresh.interval.ms"`

	TLSEnabled bool       `json:"tls.enabled,string" mapstructure:"tls.enabled"`
	TLS        *TLSConfig `json:"tls" mapstructure:"tls"`
}

func DefaultConsumerConfig() ConsumerConfig {
	c := ConsumerConfig{
		Net: NetConfig{
			ConnectTimeoutMS:    30000,
			TimeoutMS:           30000,
			TimeoutMSForEachAPI: make([]int, 0),
			KeepAliveMS:         7200000,
		},
		ClientID:             "healer",
		GroupID:              "",
		SessionTimeoutMS:     30000,
		RetryBackOffMS:       100,
		MetadataMaxAgeMS:     300000,
		FetchMaxWaitMS:       500,
		FetchMaxBytes:        10 * 1024 * 1024,
		FetchMinBytes:        1,
		FromBeginning:        false,
		AutoCommit:           true,
		AutoCommitIntervalMS: 5000,
		OffsetsStorage:       1,
	}

	c.resetTimeoutMSForEachAPI()

	return c
}

func (c *ConsumerConfig) resetTimeoutMSForEachAPI() {
	if len(c.Net.TimeoutMSForEachAPI) == 0 {
		c.Net.TimeoutMSForEachAPI = make([]int, 68)
	}
	for i := range c.Net.TimeoutMSForEachAPI {
		c.Net.TimeoutMSForEachAPI[i] = c.Net.TimeoutMS
	}
	c.Net.TimeoutMSForEachAPI[API_JoinGroup] = int(c.SessionTimeoutMS) + 5000
	c.Net.TimeoutMSForEachAPI[API_OffsetCommitRequest] = int(c.SessionTimeoutMS) / 2
	c.Net.TimeoutMSForEachAPI[API_FetchRequest] = c.Net.TimeoutMS + int(c.FetchMaxWaitMS)
}

var defaultConsumerConfig = DefaultConsumerConfig()

// create ConsumerConfig from map or return directly if config is ConsumerConfig
// return defaultConsumerConfig if config is nil
func createConsumerConfig(config interface{}) (c ConsumerConfig, err error) {
	defer func() {
		c.resetTimeoutMSForEachAPI()
	}()

	switch config := config.(type) {
	case nil:
		return defaultConsumerConfig, nil
	case map[string]interface{}:
		c = defaultConsumerConfig
		if err := mapstructure.WeakDecode(config, &c); err != nil {
			return defaultConsumerConfig, fmt.Errorf("decode consumer config error: %w", err)
		}
	case ConsumerConfig:
		return config, nil
	default:
		return c, fmt.Errorf("consumer only accept config from map[string]interface{} or ConsumerConfig")
	}
	return c, err
}

var (
	errEmptyGroupID                 = errors.New("group.id is empty")
	errInvallidOffsetsStorageConfig = errors.New("offsets.storage must be 0 or 1")
)

func (config *ConsumerConfig) checkValid() error {
	if config.BootstrapServers == "" {
		return errBootstrapServersNotSet
	}
	if config.GroupID == "" {
		return errEmptyGroupID
	}
	if config.OffsetsStorage != 0 && config.OffsetsStorage != 1 {
		return errInvallidOffsetsStorageConfig
	}
	return nil
}

// ProducerConfig is the config for producer
type ProducerConfig struct {
	Net                      NetConfig  `json:"net" mapstructure:"net"`
	Sasl                     SaslConfig `json:"sasl" mapstructure:"sasl"`
	BootstrapServers         string     `json:"bootstrap.servers" mapstructure:"bootstrap.servers"`
	ClientID                 string     `json:"client.id" mapstructure:"client.id"`
	Acks                     int16      `json:"acks,string" mapstructure:"acks"`
	CompressionType          string     `json:"compress.type" mapstructure:"compress.type"`
	BatchSize                int        `json:"batch.size,string" mapstructure:"batch.size"`
	MessageMaxCount          int        `json:"message.max.count,string" mapstructure:"message.max.count"`
	FlushIntervalMS          int        `json:"flush.interval.ms,string" mapstructure:"flush.interval.ms,string"`
	MetadataMaxAgeMS         int        `json:"metadata.max.age.ms,string" mapstructure:"metadata.max.age.ms"`
	FetchTopicMetaDataRetrys int        `json:"fetch.topic.metadata.retrys,string" mapstructure:"fetch.topic.metadata.retrys"`
	ConnectionsMaxIdleMS     int        `json:"connections.max.idle.ms,string" mapstructure:"connections.max.idle.ms"`
	RetryBackOffMS           int        `json:"retry.backoff.ms,string" mapstructure:"retry.backoff.ms"`

	MetadataRefreshIntervalMS int `json:"metadata.refresh.interval.ms,string" mapstructure:"metadata.refresh.interval.ms"`

	TLSEnabled bool       `json:"tls.enabled,string" mapstructure:"tls.enabled"`
	TLS        *TLSConfig `json:"tls" mapstructure:"tls"`

	// TODO
	Retries          int   `json:"retries,string" mapstructure:"retries"`
	RequestTimeoutMS int32 `json:"request.timeout.ms,string" mapstructure:"request.timeout.ms"`

	// producer.AddMessage will use this config to assemble Message
	// only 0 and 1 is implemented for now
	HealerMagicByte int `json:"healer.magicbyte,string" mapstructure:"healer.magicbyte"`
}

// DefaultProducerConfig returns a default ProducerConfig
func DefaultProducerConfig() ProducerConfig {
	return ProducerConfig{
		Net: NetConfig{
			ConnectTimeoutMS:    30000,
			TimeoutMS:           30000,
			TimeoutMSForEachAPI: make([]int, 0),
			KeepAliveMS:         7200000,
		},
		ClientID:                 "healer",
		Acks:                     1,
		CompressionType:          "none",
		BatchSize:                16384,
		MessageMaxCount:          1024,
		FlushIntervalMS:          200,
		MetadataMaxAgeMS:         300000,
		FetchTopicMetaDataRetrys: 3,
		ConnectionsMaxIdleMS:     540000,
		RetryBackOffMS:           200,

		TLSEnabled: false,

		Retries:          0,
		RequestTimeoutMS: 30000,
	}
}

var defaultProducerConfig = DefaultProducerConfig()

// create ProducerConfig from map or return directly if config is ProducerConfig
// return defaultProducerConfig if config is nil
func createProducerConfig(config interface{}) (c ProducerConfig, err error) {
	switch v := config.(type) {
	case nil:
		return defaultProducerConfig, nil
	case map[string]interface{}:
		c = defaultProducerConfig
		if err := mapstructure.WeakDecode(config, &c); err != nil {
			return defaultProducerConfig, fmt.Errorf("decode producer config error: %w", err)
		}
	case ProducerConfig:
		c = v
	default:
		return c, fmt.Errorf("producer only accept config from map[string]interface{} or ProducerConfig")
	}
	err = c.checkValid()
	return c, err
}

var (
	errMessageMaxCount        = errors.New("message.max.count must > 0")
	errFlushIntervalMS        = errors.New("flush.interval.ms must > 0")
	errUnknownCompressionType = errors.New("unknown compression type")
	errBootstrapServersNotSet = errors.New("bootstrap servers not set")
)

func (config *ProducerConfig) checkValid() error {
	if config.BootstrapServers == "" {
		return errBootstrapServersNotSet
	}
	if config.MessageMaxCount <= 0 {
		return errMessageMaxCount
	}
	if config.FlushIntervalMS <= 0 {
		return errFlushIntervalMS
	}

	switch config.CompressionType {
	case "none":
	case "gzip":
	case "snappy":
	case "lz4":
	default:
		return errUnknownCompressionType
	}
	return nil
}
