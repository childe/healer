package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/childe/healer"
)

var (
	brokers         = flag.String("brokers", "127.0.0.1:9092", "The list of hostname and port of the server to connect to.")
	topic           = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	partition       = flag.Int("partition", 0, "The partition to consume from.")
	compressionType = flag.String("compression.type", "none", "defalut:none")
	value           = flag.String("value", "", "")
	key             = flag.String("key", "", "")
)

func main() {
	flag.Parse()

	if *topic == "" {
		flag.PrintDefaults()
		os.Exit(4)
	}
	if *value == "" {
		flag.PrintDefaults()
		os.Exit(4)
	}

	config := make(map[string]interface{})
	config["message.max.count"] = 1
	config["bootstrap.servers"] = *brokers
	config["compression.type"] = *compressionType
	simpleProducer := healer.NewSimpleProducer(*topic, int32(*partition), config)

	if simpleProducer == nil {
		fmt.Println("could not create simpleProducer")
		os.Exit(5)
	}

	simpleProducer.AddMessage([]byte(*key), []byte(*value))
}
