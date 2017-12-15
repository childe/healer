package main

import (
	"encoding/json"
	"flag"
	"fmt"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

var (
	brokers        = flag.String("brokers", "127.0.0.1:9092", "The list of hostname and port of the server to connect to(defautl: 127.0.0.1:9092).")
	topic          = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	groupID        = flag.String("groupID", "", "REQUIRED")
	clientID       = flag.String("clientID", "healer", "The ID of this client.")
	connectTimeout = flag.Int("connect-timeout", 10, "default 10 Second. connect timeout to broker")
	timeout        = flag.Int("timeout", 30, "default 30 Second. read timeout from connection to broker")
)

func main() {
	flag.Parse()

	if *topic == "" {
		flag.PrintDefaults()
		glog.Fatal("topic needed")
	}

	brokers, err := healer.NewBrokers(*brokers, *clientID, *connectTimeout, *timeout)
	if err != nil {
		glog.Fatalf("could not create brokers from %s:%s", *brokers, err)
	}

	metadata, err := brokers.RequestMetaData(*clientID, topic)
	if err != nil {
		glog.Fatalf("could not get metadata:%s", err)
	}

	b, err := json.Marshal(metadata)
	if err != nil {
		glog.Fatalf("could not marshal metadata:%s", err)
	}
	glog.Info(string(b))

	r := healer.NewOffsetFetchRequest(0, *clientID, *groupID)
	for _, t := range metadata.TopicMetadatas {
		for _, p := range t.PartitionMetadatas {
			r.AddPartiton(t.TopicName, int32(p.PartitionId))
		}
	}

	var response []byte = nil
	for _, brokerInfo := range brokers.brokersInfo {
		broker, err := brokers.GetBroker(brokerInfo.NodeId)
		if err != nil {
			continue
		}
		response, err = broker.request(r.Encode())
		if err != nil {
			glog.Infof("request offset fetch from %s error:%s", broker.address, err)
		} else {
			break
		}
	}

	if response == nil {
		glog.Fatal("could not get offset fetch response from %s", *brokers)
	}

	var res *healer.OffsetFetchResponse
	res, err = healer.NewOffsetFetchResponse(response)
	if err != nil {
		glog.Fatal("decode offset fetch response error:%s", err)
	}

	for _, t := range res.Topics {
		for _, p := range t.Partitions {
			fmt.Printf("%s:%d:%d\n", t.Topic, p.PartitionID, p.Offset)
		}
	}
}
