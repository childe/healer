package healer

import (
	"github.com/go-logr/logr"
)

type Client struct {
	clientID string

	logger logr.Logger

	brokers *Brokers
}

// NewClient creates a new Client
func NewClient(bs, clientID string) (*Client, error) {
	var err error
	client := &Client{
		clientID: clientID,
		logger:   GetLogger().WithName(clientID),
	}
	client.brokers, err = NewBrokers(bs)
	return client, err
}

func (client *Client) WithLogger(logger logr.Logger) *Client {
	client.logger = logger
	return client
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

func (c *Client) DescribeLogDirs(topics []string) (map[int32]DescribeLogDirsResponse, error) {
	c.logger.Info("describe logdirs", "topics", topics)

	meta, err := c.brokers.RequestMetaData(c.clientID, topics)
	if err != nil {
		return nil, err
	}

	type tp struct {
		Topic       string
		PartitionID int32
	}
	brokerPartitions := make(map[int32][]tp)
	for _, topic := range meta.TopicMetadatas {
		topicName := topic.TopicName
		for _, partition := range topic.PartitionMetadatas {
			pid := partition.PartitionID
			for _, b := range partition.Replicas {
				if _, ok := brokerPartitions[b]; !ok {
					brokerPartitions[b] = []tp{
						{
							Topic:       topicName,
							PartitionID: pid,
						},
					}
				} else {
					brokerPartitions[b] = append(brokerPartitions[b], tp{
						Topic:       topicName,
						PartitionID: pid,
					})
				}
			}
		}
	}

	c.logger.Info("broker partitions", "brokerPartitions", brokerPartitions)

	rst := make(map[int32]DescribeLogDirsResponse)
	for b, topicPartitions := range brokerPartitions {
		req := NewDescribeLogDirsRequest(c.clientID, nil)
		for _, tp := range topicPartitions {
			req.AddTopicPartition(tp.Topic, tp.PartitionID)
		}

		broker, err := c.brokers.GetBroker(b)
		if err != nil {
			return nil, err
		}
		resp, err := broker.RequestAndGet(req)
		if err != nil {
			c.logger.Error(err, "describe logdirs failed", "broker", broker.String())
			continue
		}

		topicSet := make(map[string]struct{})
		for _, t := range topics {
			topicSet[t] = struct{}{}
		}
		r := resp.(DescribeLogDirsResponse)
		rs := r.Results
		for i := range rs {
			theTopics := rs[i].Topics
			filterdTopics := make([]DescribeLogDirsResponseTopic, 0)
			for i := range theTopics {
				if _, ok := topicSet[theTopics[i].TopicName]; ok {
					filterdTopics = append(filterdTopics, theTopics[i])
				}
			}
			rs[i].Topics = filterdTopics
		}

		filteredTopicResults := make([]DescribeLogDirsResponseResult, 0)
		for i := range rs {
			if len(rs[i].Topics) > 0 {
				filteredTopicResults = append(filteredTopicResults, rs[i])
			}
		}
		r.Results = filteredTopicResults
		rst[b] = resp.(DescribeLogDirsResponse)
	}

	return rst, nil
}

func (c *Client) DeleteTopics(topics []string, timeoutMs int32) (r DeleteTopicsResponse, err error) {
	c.logger.Info("delete topics", "topics", topics)

	req := NewDeleteTopicsRequest(c.clientID, topics, timeoutMs)

	controller, err := c.brokers.GetController()
	if err != nil {
		return r, err
	}

	resp, err := controller.RequestAndGet(req)
	return resp.(DeleteTopicsResponse), err
}

func (c *Client) DescribeAcls(r DescribeAclsRequestBody) (DescribeAclsResponse, error) {
	req := DescribeAclsRequest{
		RequestHeader{
			APIKey:   API_DescribeAcls,
			ClientID: &c.clientID,
		},
		r,
	}

	controller, err := c.brokers.GetController()
	if err != nil {
		return DescribeAclsResponse{}, err
	}

	resp, err := controller.RequestAndGet(&req)
	if err != nil {
		return DescribeAclsResponse{}, err
	}
	return resp.(DescribeAclsResponse), err
}
