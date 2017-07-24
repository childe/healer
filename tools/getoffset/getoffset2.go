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

	offsetsResponse, err := brokers.RequestOffset(topic, 0, *timeValue, (uint32)(*offsets))

	if err != nil {
		glog.Errorf("failed to get offsets:%s", err)
		os.Exit(5)
	}

	s, err := json.MarshalIndent(offsetsResponse, "", "  ")
	if err != nil {
		glog.Errorf("failed to marshal offsets response:%s", err)
		os.Exit(5)
	}

	fmt.Println(string(s))
}
