package main

//TODO maxBytes

import (
	"fmt"
	"math"
	"os"
	"strings"

	goflag "flag"

	flag "github.com/spf13/pflag"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

var (
	topic       = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	partition   = flag.Int("partition", 0, "The partition to consume from.")
	offset      = flag.Int64("offset", -2, "The offset id to consume from, default to -2 which means from beginning; while value -1 means from end(default -2).")
	stopOffset  = flag.Int64("stop-offset", 0, "fetch messages until stop-offset")
	maxMessages = flag.Int("max-messages", math.MaxInt32, "The number of messages to consume (default: 2147483647)")
	printOffset = flag.Bool("printoffset", true, "print offset before message")

	brokers = flag.String("brokers", "", "REQUIRED: The list of hostname and port of the server to connect to")

	config         = flag.String("config", "", "XX=YY,AA=ZZ")
	consumerConfig = map[string]interface{}{"bootstrap.servers": brokers}
)

func init() {
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)
}

func main() {
	flag.Parse()
	defer glog.Flush()

	if *topic == "" {
		flag.PrintDefaults()
		fmt.Println("need topic name")
		os.Exit(4)
	}

	for _, kv := range strings.Split(*config, ",") {
		if strings.Trim(kv, " ") == "" {
			continue
		}
		t := strings.SplitN(kv, "=", 2)
		if len(t) != 2 {
			glog.Errorf("invalid config : %s", kv)
			os.Exit(4)
		}
		consumerConfig[t[0]] = t[1]
	}
	cConfig, err := healer.GetConsumerConfig(consumerConfig)
	if err != nil {
		glog.Errorf("config error : %s", err)
		os.Exit(4)
	}

	glog.V(10).Infof("config : %+v", cConfig)
	simpleConsumer, err := healer.NewSimpleConsumer(*topic, int32(*partition), cConfig)
	if err != nil {
		glog.Errorf("crate simple consumer error: %s", err)
		os.Exit(5)
	}

	messages, err := simpleConsumer.Consume(*offset, nil)
	if err != nil {
		glog.Fatal(err)
	}

	for i := 0; i < *maxMessages; i++ {
		message := <-messages
		if message.Error != nil {
			fmt.Printf("messag error:%s\n", message.Error)
			break
		}
		if *stopOffset > 0 && message.Message.Offset >= *stopOffset {
			break
		}
		if *printOffset {
			fmt.Printf("%d: %s\n", message.Message.Offset, message.Message.Value)
		} else {
			fmt.Printf("%s\n", message.Message.Value)
		}
	}
}
