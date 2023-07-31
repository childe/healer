package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
)

func metadata() {}

var apiCmd = &cobra.Command{
	Use:   "api",
	Short: "support http api",

	RunE: func(cmd *cobra.Command, args []string) error {
		address, _ := cmd.Flags().GetString("address")
		port, _ := cmd.Flags().GetInt32("port")

		fullAddress := fmt.Sprintf("%s:%d", address, port)
		router := gin.Default()

		router.GET("/", func(c *gin.Context) {
			c.String(http.StatusOK, "")
		})

		router.GET("/health", func(c *gin.Context) {
			c.String(http.StatusOK, "")
		})

		router.GET("/metadata", func(c *gin.Context) {
			bootstrapServers := c.Query("bootstrap")
			bs, err := healer.NewBrokers(bootstrapServers)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
			topics := c.QueryArray("topics")
			resp, err := bs.RequestMetaData("healer-api", topics)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
			c.JSON(http.StatusOK, resp)
		})

		router.POST("/alter-partition-reassignments", func(c *gin.Context) {
			type reassignment struct {
				Topic     string  `json:"topic"`
				Partition int32   `json:"partition"`
				Replicas  []int32 `json:"replicas"`
			}

			bootstrapServers := c.Query("bootstrap")
			bs, err := healer.NewBrokers(bootstrapServers)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
			timeout := c.Query("timeout")
			assign := c.Query("assign")
			reassignments := make([]reassignment, 0)
			err = json.Unmarshal([]byte(assign), &reassignments)
			if err != nil {
				c.String(http.StatusBadRequest, fmt.Sprintf("assign value error: %s", err))
			}
			controller, err := bs.GetBroker(bs.Controller())
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
			}

			timeoutMS, err := strconv.Atoi(timeout)
			if err != nil {
				c.String(http.StatusBadRequest, fmt.Sprintf("timeout value error: %s", err))
			}
			req := healer.NewAlterPartitionReassignmentsRequest(int32(timeoutMS))
			for _, v := range reassignments {
				req.AddAssignment(v.Topic, v.Partition, v.Replicas)
			}
			resp, err := controller.RequestAndGet(&req)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
			}
			c.JSON(http.StatusOK, resp)
		})

		router.Run(fullAddress)
		return nil
	},
}

func init() {
	apiCmd.Flags().Int32P("port", "p", 8080, "listen port")
	apiCmd.Flags().StringP("address", "a", "0.0.0.0", "listen address")

	rootCmd.AddCommand(apiCmd)
}
