package client

import (
	"github.com/childe/healer"
	"github.com/go-logr/logr"
)

type Client struct {
	clientID string

	logger logr.Logger

	brokers *healer.Brokers
}

// New creates a new Client
func New(bs, clientID string) (*Client, error) {
	var err error
	client := &Client{
		clientID: clientID,
		logger:   healer.GetLogger().WithName(clientID),
	}
	client.brokers, err = healer.NewBrokers(bs)
	return client, err
}

// Close closes the connections to kafka brokers
func (c *Client) Close() {
	c.brokers.Close()
}

// RefreshMetadata refreshes metadata for c.brokers
func (c *Client) RefreshMetadata() {
}

// ListGroups lists all consumer groups from all brokers
func (c *Client) ListGroups() (groups []string, err error) {
	for _, brokerinfo := range c.brokers.BrokersInfo() {
		broker, err := c.brokers.GetBroker(brokerinfo.NodeID)
		if err != nil {
			c.logger.Error(err, "get broker failed", "NodeID", brokerinfo.NodeID)
			return groups, err
		}

		response, err := broker.RequestListGroups(c.clientID)
		if err != nil {
			c.logger.Error(err, "get group list failed", "broker", broker.GetAddress())
			return groups, err
		}
		for _, g := range response.Groups {
			groups = append(groups, g.GroupID)
		}
	}
	return groups, nil
}
