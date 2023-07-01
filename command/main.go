/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package main

import (
	"github.com/childe/healer/command/cmd"
	"github.com/golang/glog"
)

func main() {
	cmd.Execute()
	defer glog.Flush()
}
