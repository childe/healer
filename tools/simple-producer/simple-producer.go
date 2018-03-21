package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

var (
	config    = healer.DefaultProducerConfig
	topic     = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	partition = flag.Int("partition", 0, "The partition to consume from.")
)

func init() {
	flag.StringVar(&config.BootstrapServers, "brokers", "127.0.0.1:9092", "The list of hostname and port of the server to connect to.")
	flag.StringVar(&config.CompressionType, "compression.type", "none", "defalut:none")
}

func main() {
	flag.Parse()

	if *topic == "" {
		flag.PrintDefaults()
		os.Exit(4)
	}

	simpleProducer := healer.NewSimpleProducer(*topic, int32(*partition), config)

	if simpleProducer == nil {
		fmt.Println("could not create simpleProducer")
		os.Exit(5)
	}

	var (
		text     []byte = nil
		line     []byte = nil
		isPrefix bool   = true
		err      error  = nil
	)
	reader := bufio.NewReader(os.Stdin)
	for {
		text = nil
		isPrefix = true
		for isPrefix {
			line, isPrefix, err = reader.ReadLine()
			if err != nil {
				if err == io.EOF {
					os.Exit(0)
				}
				glog.Errorf("readline error:%s", err)
				os.Exit(5)
			}
			if text == nil {
				text = line
			} else {
				text = append(text, line...)
			}
		}
		simpleProducer.AddMessage(nil, text)
	}
}
