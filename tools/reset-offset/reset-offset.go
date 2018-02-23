package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

var (
	brokersList    = flag.String("brokers", "127.0.0.1:9092", "The list of hostname and port of the server to connect to(defautl: 127.0.0.1:9092).")
	topic          = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	offsetsStorage = flag.String("offsets.storage", "kafka", "default kafka. Select where offsets should be stored (zookeeper or kafka).")
	clientID       = flag.String("clientID", "healer", "The ID of this client. default healer")
	groupID        = flag.String("groupID", "", "REQUIRED: The ID of this client.")
	connectTimeout = flag.Int("connect-timeout", 30, "default 30 Second. connect timeout to broker")
	timeout        = flag.Int("timeout", 60, "default 10 Second. read timeout from connection to broker")
	timestamp      = flag.Int64("timestamp", -3, "REQUIRED: -2 which means beginning; -1 means end.")
)

func main() {
	flag.Parse()

	if *topic == "" {
		flag.PrintDefaults()
		fmt.Println("need topic name")
		os.Exit(4)
	}

	if *groupID == "" {
		flag.PrintDefaults()
		fmt.Println("need group name")
		os.Exit(4)
	}

	if *timestamp < -2 {
		flag.PrintDefaults()
		fmt.Printf("illegel timestamp:%d\n", *timestamp)
		os.Exit(4)
	}

	var (
		err     error
		brokers *healer.Brokers
	)
	brokers, err = healer.NewBrokers(*brokersList, *clientID, *connectTimeout, *timeout)
	if err != nil {
		glog.Fatalf("failed to create brokers:%s", err)
	}

	metaDataResponse, err := brokers.RequestMetaData(*clientID, topic)
	if err != nil {
		glog.Fatalf("could not get metadata:%s", err)
	}

	b, _ := json.Marshal(metaDataResponse)
	glog.Infof("topic[%s] metadata:%s", *topic, b)

	// only one topic
	topicMetadata := metaDataResponse.TopicMetadatas[0]

	var (
		partitionID int32
		offset      int64
		offsets     map[int32]int64 = make(map[int32]int64)
	)
	for _, partitionMetadata := range topicMetadata.PartitionMetadatas {
		partitionID = int32(partitionMetadata.PartitionId)
		if partitionMetadata.PartitionErrorCode != 0 {
			glog.Fatalf("get partition[%d] metainfo errorcode:%d", partitionID, partitionMetadata.PartitionErrorCode)
		}

		// get offset
		offsetsResponses, err := brokers.RequestOffsets(*clientID, *topic, int32(partitionID), *timestamp, 3)
		if err != nil {
			glog.Fatalf("could not get offsets:%s", err)
		}
		for _, offsetsResponse := range offsetsResponses {
			for topic, partitionOffsets := range offsetsResponse.Info {
				for _, partitionOffset := range partitionOffsets {
					partition := partitionOffset.Partition
					_offsets := partitionOffset.Offset
					errorCode := partitionOffset.ErrorCode
					if errorCode != 0 {
						glog.Fatalf("get offset[%s:%d] error:%d", topic, partition, errorCode)
					}
					if len(_offsets) == 0 {
						glog.Fatalf("could not get offsets:`%s[%d]`", topic, partition)
					}
					glog.Infof("%s:%d:%v", topic, partition, _offsets)
					offset = int64(_offsets[0])
					offsets[partitionID] = offset
				}
			}
		}
	}

	// commit offset

	// 1. get coordinator
	var coordinator *healer.Broker
	coordinatorResponse, err := brokers.FindCoordinator(*clientID, *groupID)
	if err != nil {
		glog.Fatalf("failed to find coordinator:%s", err)
	}

	coordinator, err = brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
	if err != nil {
		glog.Fatalf("could not get broker[%d]:%s", coordinatorResponse.Coordinator.NodeID, err)
	}
	glog.Infof("coordinator for group[%s]:%s", *groupID, coordinator.GetAddress())

	// 2. join
	var (
		protocolType   string = "consumer"
		memberID       string = ""
		generationID   int32
		sessionTimeout int32 = 30000
	)

	protocolMetadata := &healer.ProtocolMetadata{
		Version:      0,
		Subscription: []string{*topic},
		UserData:     nil,
	}

	gps := []*healer.GroupProtocol{&healer.GroupProtocol{"range", protocolMetadata.Encode()}}
	joinGroupRequest := healer.NewJoinGroupRequest(*clientID, *groupID, sessionTimeout, memberID, protocolType)
	for _, gp := range gps {
		joinGroupRequest.AddGroupProtocal(gp)
	}

	responseBytes, err := coordinator.Request(joinGroupRequest)
	if err != nil {
		glog.Fatalf("request joingroup error:%s", err)
	}

	joinGroupResponse, err := healer.NewJoinGroupResponse(responseBytes)
	if err != nil {
		glog.Fatalf("get join group response error:%s", err)
	}

	generationID = joinGroupResponse.GenerationID
	memberID = joinGroupResponse.MemberID
	glog.Infof("generationID:%d memberID:%s", generationID, memberID)

	// 3. sync
	var groupAssignment healer.GroupAssignment = nil
	syncGroupRequest := healer.NewSyncGroupRequest(*clientID, *groupID, generationID, memberID, groupAssignment)

	responseBytes, err = coordinator.Request(syncGroupRequest)
	if err != nil {
		glog.Fatalf("request sync api error:%s", err)
	}

	_, err = healer.NewSyncGroupResponse(responseBytes)

	if err != nil {
		glog.Fatalf("decode sync response error:%s", err)
	}

	// 4. commit
	var (
		apiVersion uint16
	)
	if *offsetsStorage == "zookeeper" {
		apiVersion = 0
	} else {
		apiVersion = 2
	}
	offsetComimtReq := healer.NewOffsetCommitRequest(apiVersion, *clientID, *groupID)
	offsetComimtReq.SetMemberID(memberID)
	offsetComimtReq.SetGenerationID(generationID)
	offsetComimtReq.SetRetentionTime(-1)
	for partitionID, offset = range offsets {
		offsetComimtReq.AddPartiton(*topic, partitionID, offset, "")
	}

	payload, err := coordinator.Request(offsetComimtReq)
	if err != nil {
		glog.Infof("commit offset [%s][%d]:%d error:%s", topic, partitionID, offset, err)
		return
	}

	_, err = healer.NewOffsetCommitResponse(payload)
	if err == nil {
		glog.Infof("commit offset [%s][%d]:%d", *topic, partitionID, offset)
	} else {
		glog.Infof("commit offset [%s][%d]:%d error:%s", *topic, partitionID, offset, err)
	}
}
