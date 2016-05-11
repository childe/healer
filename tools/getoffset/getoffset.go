package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/childe/gokafka"
)

var (
	brokerList = flag.String("brokers", "127.0.0.1:9092", "<hostname:port,...,hostname:port> The comma separated list of brokers in the Kafka cluster. (default: 127.0.0.1:9092)")
	topic      = flag.String("topic", "", "REQUIRED: The topic to get offset from.")
	timeValue  = flag.Int64("time", -1, "timestamp/-1(latest)/-2(earliest). timestamp of the offsets before that.(default: -1) ")
	offsets    = flag.Uint("offsets", 1, "number of offsets returned (default: 1)")
	clientID   = flag.String("clientID", "gokafka", "The ID of this client.")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	if *topic == "" {
		fmt.Println("need topic!")
		flag.PrintDefaults()
	}

	correlationID := int32(os.Getpid())
	metadataResponse, err := gokafka.GetMetaData(*brokerList, *topic, int32(correlationID), *clientID)
	if err != nil {
		logger.Fatal(err)
	}

	brokers := metadataResponse.Brokers

	partitions := metadataResponse.TopicMetadatas[0].PartitionMetadatas

	for _, partition := range partitions {
		partitionID := partition.PartitionId
		leader := partition.Leader

		for _, broker := range brokers {
			if leader == broker.NodeId {
				brokerAddr := net.JoinHostPort(broker.Host, strconv.Itoa(int(broker.Port)))
				offsetResponse, err := gokafka.GetOffset(brokerAddr, *topic, partitionID, correlationID, *clientID, *timeValue, uint32(*offsets))
				if err != nil {
					fmt.Println(err)

					//next partition
					break
				}
				for topic, partitions := range offsetResponse.Info {
					for _, partition := range partitions {
						fmt.Printf("%s:%d", topic, partition.Partition)
						for _, offset := range partition.Offset {
							fmt.Printf(":%d", offset)
						}
						fmt.Println()
					}
				}
			}
		}
	}
}
