package main

import (
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

var (
	brokerList = flag.String("brokers", "127.0.0.1:9092", "<hostname:port,...,hostname:port> The comma separated list of brokers in the Kafka cluster. (default: 127.0.0.1:9092)")
	topic      = flag.String("topic", "", "REQUIRED: The topic to get offset from.")
	timestamp  = flag.Int64("timestamp", -1, "timestamp/-1(latest)/-2(earliest). timestamp of the offsets before that.(default: -1) ")
	offsets    = flag.Uint("offsets", 1, "number of offsets returned (default: 1)")
	clientID   = flag.String("clientID", "healer", "The ID of this client.")
	format     = flag.String("format", "", "output original kafka response if set to original")
)

func main() {
	flag.Parse()

	if *topic == "" {
		glog.Error("need topic")
		flag.PrintDefaults()
		os.Exit(4)
	}

	brokers, err := healer.NewBrokers(*brokerList)
	if err != nil {
		glog.Errorf("create brokers error:%s", err)
		os.Exit(5)
	}

	offsetsResponse, err := brokers.RequestOffsets(*clientID, *topic, -1, *timestamp, (uint32)(*offsets))

	if err != nil {
		glog.Errorf("failed to get offsets:%s", err)
		os.Exit(5)
	}

	rst := make([]*healer.PartitionOffset, 0)

	for _, x := range offsetsResponse {
		for _, partitionOffsetsList := range x.TopicPartitionOffsets {
			rst = append(rst, partitionOffsetsList...)
		}
	}

	sort.Slice(rst, func(i, j int) bool { return rst[i].Partition < rst[j].Partition })
	for _, partitionOffset := range rst {
		fmt.Printf("%s:%d:", *topic, partitionOffset.Partition)
		for i, offset := range partitionOffset.Offsets {
			if i != 0 {
				fmt.Print(",")
			}
			fmt.Printf("%d", offset)
		}
		fmt.Println()
	}
}
