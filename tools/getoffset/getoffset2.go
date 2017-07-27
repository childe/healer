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
	brokerList = flag.String("brokers", "127.0.0.1:9092", "<hostname:port,...,hostname:port> The comma separated list of brokers in the Kafka cluster. (default: 127.0.0.1:9092)")
	topic      = flag.String("topic", "", "REQUIRED: The topic to get offset from.")
	timeValue  = flag.Int64("time", -1, "timestamp/-1(latest)/-2(earliest). timestamp of the offsets before that.(default: -1) ")
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

	brokers, err := healer.NewBrokers(*brokerList, *clientID)
	if err != nil {
		glog.Errorf("create brokers error:%s", err)
		os.Exit(5)
	}

	offsetsResponse, err := brokers.RequestOffsets(*topic, -1, *timeValue, (uint32)(*offsets))

	if err != nil {
		glog.Errorf("failed to get offsets:%s", err)
		os.Exit(5)
	}

	s, err := json.MarshalIndent(offsetsResponse, "", "  ")
	if err != nil {
		glog.Errorf("failed to marshal offsets response:%s", err)
		os.Exit(5)
	}

	if *format == "original" {
		fmt.Println(string(s))
	} else {
		for _, x := range offsetsResponse {
			for topic, partitionOffsetsList := range x.Info {
				for _, partitionOffsets := range partitionOffsetsList {
					fmt.Printf("%s:%d:", topic, partitionOffsets.Partition)
					for i, offset := range partitionOffsets.Offset {
						if i != 0 {
							fmt.Print(",")
						}
						fmt.Printf("%d", offset)
					}
					fmt.Println()
				}
			}
		}
	}
}
