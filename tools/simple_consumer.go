package main

import (
	"flag"
	//"fmt"
	"github.com/gokafka"
	"log"
	//"net"
	"os"
	"strings"
	//"time"
)

var (
	brokerList  = flag.String("brokers", "127.0.0.1:9092", "The list of hostname and port of the server to connect to.")
	topic       = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	partition   = flag.Int("partition", 0, "The partition to consume from.")
	offset      = flag.Int64("offset", -1, "The offset id to consume from, default to -2 which means from beginning; while value -1 means from end")
	clientId    = flag.String("clientId", "gokafkaShell", "The ID of this client.")
	fetchSize   = flag.Int("fetchsize", -1, "The fetch size of each request.")
	maxWaitMs   = flag.Int("max-wait-ms", -1, "The fetch size of each request.")
	maxMessages = flag.Int("max-messages", -1, "The fetch size of each request.")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	pid := os.Getpid()
	//fmt.Println("pid:", pid)

	fetchRequest := gokafka.FetchRequest{}

	fetchRequest.RequestHeader = &gokafka.RequestHeader{
		ApiKey:        gokafka.API_MetadataRequest,
		ApiVersion:    0,
		CorrelationId: uint32(pid),
		ClientId:      *clientId,
	}

	var metadataResponse *gokafka.MetadataResponse = nil
	for _, broker := range strings.Split(*brokerList, ",") {
		_metadataResponse, err := gokafka.GetMetaData(broker, *topic, int32(pid), *clientId)
		if err != nil {
			logger.Println(err)
			continue
		}
		metadataResponse = _metadataResponse
		break
	}

	if metadataResponse == nil {
		logger.Fatalf("cou = _metadataResponseld not get metadata from ")
	}
}
