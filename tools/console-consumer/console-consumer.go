package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	goflag "flag"

	flag "github.com/spf13/pflag"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

var (
	topic       = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	partitions  = flag.String("partitions", "", "comma separated. consume all partitions if not set")
	maxMessages = flag.Int("max-messages", math.MaxInt32, "The number of messages to consume (default: 2147483647)")

	brokers        = flag.String("brokers", "", "REQUIRED: The list of hostname and port of the server to connect to")
	config         = flag.String("config", "", "XX=YY,AA=ZZ")
	consumerConfig = map[string]interface{}{"bootstrap.servers": brokers}
)

func init() {
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)

}

func main() {
	flag.Parse()

	if *topic == "" {
		flag.PrintDefaults()
		fmt.Println("need topic name")
		os.Exit(4)
	}

	var (
		consumer *healer.Consumer
		err      error
	)

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

	if *partitions == "" {
		consumer, err = healer.NewConsumer(cConfig, *topic)
	} else {
		assign := make(map[string][]int)
		assign[*topic] = make([]int, 0)
		for _, p := range strings.Split(*partitions, ",") {
			pid, err := strconv.Atoi(p)
			if err != nil {
				glog.Errorf("invalid partitions: %s", err)
				os.Exit(4)
			}
			assign[*topic] = append(assign[*topic], pid)
		}
		consumer, err = healer.NewConsumer(cConfig)
		consumer.Assign(assign)
	}

	if err != nil {
		glog.Errorf("create consumer error: %s", err)
		os.Exit(5)
	}

	messages, err := consumer.Consume()
	if err != nil {
		glog.Fatal(err)
	}

	for i := 0; i < *maxMessages; i++ {
		message := <-messages
		fmt.Printf("%d: %s\n", message.Message.Offset, message.Message.Value)
	}
}
