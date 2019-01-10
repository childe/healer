package main

import (
	"bufio"
	"flag"
	"io"
	"os"
	"strings"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

var (
	topic   = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	brokers = flag.String("brokers", "127.0.0.1:9092", "The list of hostname and port of the server to connect to.")
	config  = flag.String("config", "", "XX=YY,AA=ZZ")

	producerConfig = map[string]interface{}{"bootstrap.servers": brokers}
)

func init() {
}

func main() {
	flag.Parse()

	if *topic == "" {
		flag.PrintDefaults()
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
		producerConfig[t[0]] = t[1]
	}
	pConfig, err := healer.GetProducerConfig(producerConfig)
	if err != nil {
		glog.Errorf("config error : %s", err)
		os.Exit(4)
	}
	producer := healer.NewProducer(*topic, pConfig)

	if producer == nil {
		glog.Error("could not create producer")
		os.Exit(5)
	}

	var (
		text     []byte = nil
		line     []byte = nil
		isPrefix bool   = true
	)
	reader := bufio.NewReader(os.Stdin)
	for {
		text = nil
		isPrefix = true
		for isPrefix {
			line, isPrefix, err = reader.ReadLine()
			if err != nil {
				if err == io.EOF {
					producer.Close()
					os.Exit(0)
				}
				glog.Errorf("readline error:%s", err)
				os.Exit(5)
			}
			text = append(text, line...)
		}
		producer.AddMessage(nil, text)
	}
}
