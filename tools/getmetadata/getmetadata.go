package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/childe/gokafka"
)

var (
	brokerList = flag.String("brokers", "127.0.0.1:9092", "REQUIRED: The list of hostname and port of the server to connect to.")
	clientID   = flag.String("clientID", "gokafka", "The ID of this client.")
	topic      = flag.String("topic", "", "REQUIRED: The topic to get offset from.")
	logger     = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	if *topic == "" {
		fmt.Println("need topic!")
		flag.PrintDefaults()
	}

	pid := os.Getpid()
	metadataResponse, err := gokafka.GetMetaData(*brokerList, *topic, int32(pid), *clientID)
	if err != nil {
		logger.Println(err)
	}

	s, err := json.MarshalIndent(metadataResponse, "", "  ")
	if err != nil {
		logger.Println(err)
	}

	fmt.Println(string(s))
}
