package main

import (
	"flag"

	"github.com/childe/healer"
	"github.com/childe/healer/command/cmd"
	"github.com/spf13/pflag"
	"k8s.io/klog/v2"
)

func main() {
	klog.InitFlags(flag.CommandLine)
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	flag.Parse()

	defer klog.Flush()
	healer.SetLogger(klog.NewKlogr())

	cmd.Execute()
}
