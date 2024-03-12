package cmd

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
)

var apiCmd = &cobra.Command{
	Use:   "api",
	Short: "support http api",

	RunE: func(cmd *cobra.Command, args []string) error {
		address, _ := cmd.Flags().GetString("address")
		port, _ := cmd.Flags().GetInt32("port")
		client, _ := cmd.Flags().GetString("client")
		if len(client) == 0 {
			client = "healer"
		}

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
				if errors.As(err, &healer.AllError[0]) {
					c.JSON(http.StatusOK, resp)
					return
				}
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
			r := healer.NewDescribeConfigsRequest(client, resources)

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
			r := healer.NewDescribeConfigsRequest(client, resources)

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

			r := healer.NewIncrementalAlterConfigsRequest(client)
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

			timeout := c.Query("timeout")
			timeoutMS, err := strconv.Atoi(timeout)
			if err != nil {
				c.String(http.StatusBadRequest, fmt.Sprintf("timeout value error: %s", err))
				return
			}

			bootstrapServers := c.Query("bootstrap")
			config := healer.DefaultBrokerConfig()
			config.NetConfig.TimeoutMSForEachAPI = make([]int, 68)
			config.NetConfig.TimeoutMSForEachAPI[healer.API_AlterPartitionReassignments] = timeoutMS
			bs, err := healer.NewBrokersWithConfig(bootstrapServers, config)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
			defer bs.Close()

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

			timeout := c.Query("timeout")
			timeoutMS, err := strconv.Atoi(timeout)
			if err != nil {
				c.String(http.StatusBadRequest, fmt.Sprintf("timeout value error: %s", err))
				return
			}

			config := healer.DefaultBrokerConfig()
			config.NetConfig.TimeoutMSForEachAPI = make([]int, 68)
			config.NetConfig.TimeoutMSForEachAPI[healer.API_ListPartitionReassignments] = timeoutMS
			bootstrapServers := c.Query("bootstrap")
			bs, err := healer.NewBrokersWithConfig(bootstrapServers, config)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
			defer bs.Close()
			reassignments := make([]reassignment, 0)
			if err := c.BindJSON(&reassignments); err != nil {
				c.String(http.StatusBadRequest, fmt.Sprintf("reassignments value error: %s", err))
				return
			}
			req := healer.NewListPartitionReassignmentsRequest(client, int32(timeoutMS))
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

		router.POST("/create-partitions", func(c *gin.Context) {
			bootstrapServers := c.Query("bootstrap")
			bs, err := healer.NewBrokers(bootstrapServers)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
			defer bs.Close()

			count := c.Query("count")
			countI, err := strconv.Atoi(count)
			if err != nil {
				c.String(http.StatusBadRequest, fmt.Sprintf("count value error: %s", err))
				return
			}

			timeout := c.Query("timeout")
			timeoutMS, err := strconv.Atoi(timeout)
			if err != nil {
				c.String(http.StatusBadRequest, fmt.Sprintf("timeout value error: %s", err))
				return
			}

			topic := c.Query("topic")

			req := healer.NewCreatePartitionsRequest(client, uint32(timeoutMS), false)
			req.AddTopic(topic, int32(countI), nil)

			controller, err := bs.GetBroker(bs.Controller())
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
			}

			resp, err := controller.RequestAndGet(req)
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
