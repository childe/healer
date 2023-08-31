package cmd

import (
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
			defer bs.Close()
			topics := c.QueryArray("topics")
			resp, err := bs.RequestMetaData("healer-api", topics)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
			c.JSON(http.StatusOK, resp)
		})

		router.GET("/topic/:topic/configs", func(c *gin.Context) {
			bootstrapServers := c.Query("bootstrap")
			bs, err := healer.NewBrokers(bootstrapServers)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
			defer bs.Close()

			topic := c.Param("topic")

			resources := []*healer.DescribeConfigsRequestResource{
				{
					ResourceType: healer.ConvertConfigResourceType("topic"),
					ResourceName: topic,
					ConfigNames:  nil,
				},
			}
			r := healer.NewDescribeConfigsRequest("healer-api", resources)

			controller, err := bs.GetBroker(bs.Controller())
			if err != nil {
				err = fmt.Errorf("failed to create crotroller broker: %w", err)
				c.String(http.StatusInternalServerError, err.Error())
			}
			resp, err := controller.RequestAndGet(r)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
			} else {
				c.JSON(http.StatusOK, resp)
			}
		})
		router.GET("/topic/:topic/config/:config", func(c *gin.Context) {
			bootstrapServers := c.Query("bootstrap")
			bs, err := healer.NewBrokers(bootstrapServers)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
			defer bs.Close()

			topic := c.Param("topic")
			config := c.Param("config")

			resources := []*healer.DescribeConfigsRequestResource{
				{
					ResourceType: healer.ConvertConfigResourceType("topic"),
					ResourceName: topic,
					ConfigNames:  []string{config},
				},
			}
			r := healer.NewDescribeConfigsRequest("healer-api", resources)

			controller, err := bs.GetBroker(bs.Controller())
			if err != nil {
				err = fmt.Errorf("failed to create crotroller broker: %w", err)
				c.String(http.StatusInternalServerError, err.Error())
			}
			resp, err := controller.RequestAndGet(r)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
			} else {
				c.JSON(http.StatusOK, resp)
			}
		})

		router.POST("/topic/:topic/config/:config/:value", func(c *gin.Context) {
			bootstrapServers := c.Query("bootstrap")
			bs, err := healer.NewBrokers(bootstrapServers)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
			defer bs.Close()

			topic := c.Param("topic")
			config := c.Param("config")
			value := c.Param("value")

			r := healer.NewIncrementalAlterConfigsRequest("healer-api")
			r.AddConfig(healer.ConvertConfigResourceType("topic"), topic, config, value)

			controller, err := bs.GetBroker(bs.Controller())
			if err != nil {
				err = fmt.Errorf("failed to create crotroller broker: %w", err)
				c.String(http.StatusInternalServerError, err.Error())
			}
			resp, err := controller.RequestAndGet(r)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
			} else {
				c.JSON(http.StatusOK, resp)
			}
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
			defer bs.Close()

			timeout := c.Query("timeout")
			timeoutMS, err := strconv.Atoi(timeout)
			if err != nil {
				c.String(http.StatusBadRequest, fmt.Sprintf("timeout value error: %s", err))
				return
			}
			reassignments := make([]reassignment, 0)
			if err := c.BindJSON(&reassignments); err != nil {
				c.String(http.StatusBadRequest, fmt.Sprintf("reassignments value error: %s", err))
				return
			}
			req := healer.NewAlterPartitionReassignmentsRequest(int32(timeoutMS))
			for _, v := range reassignments {
				req.AddAssignment(v.Topic, v.Partition, v.Replicas)
			}
			resp, err := bs.AlterPartitionReassignments(&req)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
			c.JSON(http.StatusOK, resp)
		})

		router.POST("/list-partition-reassignments", func(c *gin.Context) {
			type reassignment struct {
				Topic     string `json:"topic"`
				Partition int32  `json:"partition"`
			}

			bootstrapServers := c.Query("bootstrap")
			bs, err := healer.NewBrokers(bootstrapServers)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
			defer bs.Close()

			timeout := c.Query("timeout")
			timeoutMS, err := strconv.Atoi(timeout)
			if err != nil {
				c.String(http.StatusBadRequest, fmt.Sprintf("timeout value error: %s", err))
				return
			}
			reassignments := make([]reassignment, 0)
			if err := c.BindJSON(&reassignments); err != nil {
				c.String(http.StatusBadRequest, fmt.Sprintf("reassignments value error: %s", err))
				return
			}
			req := healer.NewListPartitionReassignmentsRequest("healer", int32(timeoutMS))
			for _, v := range reassignments {
				req.AddTP(v.Topic, v.Partition)
			}
			resp, err := bs.ListPartitionReassignments(req)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
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
