package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

var (
	brokerConfig = healer.DefaultBrokerConfig()

	bootstrapServers = flag.String("bootstrap.servers", "127.0.0.1:9092", "The list of hostname and port of the server to connect to(defautl: 127.0.0.1:9092).")
	topic            = flag.String("topic", "", "if topic is left blank: 1.get topics under the groupID if groupID if given. 2.get all topic in the cluster if groupID is not given")
	groupID          = flag.String("group.id", "", "if groupID is left blank: 1.get all groupID consuming the topic if topic is given. 2.get all groupID in the cluster")
	clientID         = flag.String("client.id", "healer-get-pending", "The ID of this client.")

	header = flag.Bool("header", true, "if print header")
	total  = flag.Bool("total", false, "if print total offset of one topic")
)

var (
	groups = map[string]*healer.Broker{}

	brokers *healer.Brokers
	err     error
	helper  *healer.Helper
)

func init() {
	flag.IntVar(&brokerConfig.ConnectTimeoutMS, "connect-timeout", brokerConfig.ConnectTimeoutMS, fmt.Sprintf("connect timeout to broker. default %d", brokerConfig.ConnectTimeoutMS))
	flag.IntVar(&brokerConfig.TimeoutMS, "timeout", brokerConfig.TimeoutMS, fmt.Sprintf("read timeout from connection to broker. default %d", brokerConfig.TimeoutMS))
}

func getPartitions(topic string) ([]int32, error) {
	var metadataResponse *healer.MetadataResponse
	metadataResponse, err = brokers.RequestMetaData(*clientID, []string{topic})

	if err != nil {
		return nil, err
	}

	partitions := make([]int32, 0)
	for _, topicMetadata := range metadataResponse.TopicMetadatas {
		for _, partitionMetadata := range topicMetadata.PartitionMetadatas {
			partitions = append(partitions, partitionMetadata.PartitionID)
		}
	}

	return partitions, nil
}

func getOffset(topic string) (map[int32]int64, error) {
	var (
		partitionID int32 = -1
		timestamp   int64 = -1
	)
	offsetsResponses, err := brokers.RequestOffsets(*clientID, topic, partitionID, timestamp, 1)
	if err != nil {
		return nil, err
	}

	rst := make(map[int32]int64)
	for _, offsetsResponse := range offsetsResponses {
		for _, partitionOffsets := range offsetsResponse.TopicPartitionOffsets {
			for topic, partitionOffset := range partitionOffsets {
				if len(partitionOffset.Offsets) != 1 {
					return nil, fmt.Errorf("%s[%d] offsets return more than 1 value", topic, partitionOffset.Partition)
				}
				rst[partitionOffset.Partition] = partitionOffset.Offsets[0]
			}
		}
	}
	return rst, nil
}

// TODO remove partitons parameters
func getCommittedOffset(topic string, partitions []int32, groupID string) (map[int32]int64, error) {
	if _, ok := groups[groupID]; !ok {
		coordinatorResponse, err := brokers.FindCoordinator(*clientID, groupID)
		if err != nil {
			return nil, err
		}
		coordinator, err := brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
		if err != nil {
			return nil, err
		}
		glog.V(5).Infof("coordinator of %s:%s", groupID, coordinator.GetAddress())
		groups[groupID] = coordinator
	}

	r := healer.NewOffsetFetchRequest(1, *clientID, groupID)
	for _, p := range partitions {
		r.AddPartiton(topic, p)
	}

	response, err := groups[groupID].Request(r)
	if err != nil {
		return nil, err
	}

	res, err := healer.NewOffsetFetchResponse(response)
	if err != nil {
		return nil, err
	}

	rst := make(map[int32]int64)
	for _, t := range res.Topics {
		for _, p := range t.Partitions {
			rst[p.PartitionID] = p.Offset
		}
	}
	return rst, nil
}

// topicName -> partitionID -> memberID
func getSubscriptionsInGroup(groupID string) (map[string]map[int32]string, error) {
	if _, ok := groups[groupID]; !ok {
		coordinatorResponse, err := brokers.FindCoordinator(*clientID, groupID)
		if err != nil {
			glog.Fatalf("could not find coordinator: %s", err)
		}
		coordinator, err := brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
		if err != nil {
			glog.Fatalf("get broker error: %s", err)
		}
		glog.V(5).Infof("coordinator of %s:%s", groupID, coordinator.GetAddress())
		groups[groupID] = coordinator
	}

	req := healer.NewDescribeGroupsRequest(*clientID, []string{groupID})

	responseBytes, err := groups[groupID].Request(req)
	if err != nil {
		return nil, err
	}

	response, err := healer.NewDescribeGroupsResponse(responseBytes)
	if err != nil {
		return nil, err
	}

	subscriptions := make(map[string]map[int32]string)
	for _, group := range response.Groups {
		for _, memberDetail := range group.Members {
			memberID := memberDetail.MemberID
			if len(memberDetail.MemberAssignment) == 0 {
				continue
			}
			memberAssignment, err := healer.NewMemberAssignment(memberDetail.MemberAssignment)
			if err != nil {
				return nil, err
			}
			for _, p := range memberAssignment.PartitionAssignments {
				topicName := p.Topic
				if _, ok := subscriptions[topicName]; !ok {
					subscriptions[topicName] = make(map[int32]string)
				}
				for _, partitionID := range p.Partitions {
					subscriptions[topicName][partitionID] = memberID
				}
			}
		}
	}
	return subscriptions, nil
}

func getGroups(groupID string) []string {
	if groupID == "" {
		return helper.GetGroups()
	}
	if !strings.Contains(groupID, "*") {
		return []string{groupID}
	}

	var p string = strings.Replace(groupID, ".", `\.`, -1)
	p = strings.Replace(groupID, "*", ".*", -1)
	var groupPattern *regexp.Regexp = regexp.MustCompile("^" + p + "$")

	var (
		rst      []string = make([]string, 0)
		groupIDs []string = helper.GetGroups()
	)
	if groupIDs == nil {
		return nil
	}
	for _, g := range groupIDs {
		if groupPattern.MatchString(g) {
			rst = append(rst, g)
		}
	}
	return rst
}

func main() {
	flag.Parse()

	brokers, err = healer.NewBrokers(*bootstrapServers, *clientID, brokerConfig)
	if err != nil {
		glog.Errorf("create brokers error:%s", err)
		os.Exit(5)
	}

	helper, err = healer.NewHelper(*bootstrapServers, *clientID, brokerConfig)
	if err != nil {
		glog.Errorf("create helper error:%s", err)
		os.Exit(5)
	}

	groupIDs := getGroups(*groupID)
	if glog.V(5) {
		for i, group := range groupIDs {
			glog.Infof("%d/%d %s", i, len(groupIDs), group)
		}
	}

	for i, groupID := range groupIDs {
		glog.V(10).Infof("%d/%d %s", i, len(groupIDs), groupID)
		subscriptions, err := getSubscriptionsInGroup(groupID)
		if err != nil {
			glog.Errorf("get subscriptions of %s error: %s", groupID, err)
			continue
		}
		for topicName, v := range subscriptions {
			if *topic != "" && topicName != *topic {
				continue
			}

			timestamp := time.Now().Unix()

			offsets, err := getOffset(topicName)
			if err != nil {
				glog.Errorf("get offsets error: %s", err)
				continue
			}

			partitions, err := getPartitions(topicName)
			if err != nil {
				glog.Errorf("get partitions of %s error: %s", topicName, err)
				continue
			}
			committedOffsets, err := getCommittedOffset(topicName, partitions, groupID)
			if err != nil {
				glog.Errorf("get committed offsets [%s/%s] error: %s", groupID, topicName, err)
				continue
			}

			var (
				offsetSum    int64 = 0
				committedSum int64 = 0
				pendingSum   int64 = 0
			)

			if *header {
				fmt.Println("timestamp\ttopic\tgroupID\tpid\toffset\tcommited\tlag\towner")
			}
			for _, partitionID := range partitions {
				pending := offsets[partitionID] - committedOffsets[partitionID]
				offsetSum += offsets[partitionID]
				committedSum += committedOffsets[partitionID]
				pendingSum += pending
				fmt.Printf("%d\t%s\t%s\t%d\t%d\t%d\t%d\t%s\n", timestamp, topicName, groupID, partitionID, offsets[partitionID], committedOffsets[partitionID], pending, v[partitionID])
			}
			if *total {
				fmt.Printf("TOTAL\t%s\t%d\t%d\t%d\n", topicName, offsetSum, committedSum, pendingSum)
			}
		}

	}
}
