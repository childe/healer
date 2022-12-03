package main

import (
	"flag"
	"fmt"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

var (
	brokers  = flag.String("brokers", "127.0.0.1:9092", "The list of hostname and port of the server to connect to(defautl: 127.0.0.1:9092).")
	topic    = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	groupID  = flag.String("groupID", "", "REQUIRED")
	clientID = flag.String("clientID", "healer", "The ID of this client.")
)

func main() {
	flag.Parse()

	if *topic == "" {
		flag.PrintDefaults()
		fmt.Println("topic needed")
		return
	}

	if *groupID == "" {
		flag.PrintDefaults()
		fmt.Println("groupID needed")
		return
	}

	brokers, err := healer.NewBrokers(*brokers)
	if err != nil {
		glog.Fatalf("could not create brokers from %v: %v", *brokers, err)
	}

	metadata, err := brokers.RequestMetaData(*clientID, []string{*topic})
	if err != nil {
		glog.Fatalf("could not get metadata:%s", err)
	}

	r := healer.NewOffsetFetchRequest(0, *clientID, *groupID)
	for _, t := range metadata.TopicMetadatas {
		for _, p := range t.PartitionMetadatas {
			r.AddPartiton(t.TopicName, p.PartitionID)
		}
	}

	response, err := brokers.Request(r)

	if err != nil {
		glog.Fatalf("could not get offset fetch response from %s", *brokers)
	}

	res, err := healer.NewOffsetFetchResponse(response)
	if res == nil {
		glog.Fatalf("decode offset fetch response error:%s", err)
	}

	for _, t := range res.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode == 0 {
				fmt.Printf("%s:%d:%d\n", t.Topic, p.PartitionID, p.Offset)
			} else {
				// TODO: print error message
				fmt.Printf("%s:%d:%d\n", t.Topic, p.PartitionID, p.Offset)
			}
		}
	}
}
