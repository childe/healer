package cmd

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/childe/healer/command/cmd/apicontrollers"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
)

var wrap = func(f func(c *gin.Context, client string), client string) func(c *gin.Context) {
	return func(c *gin.Context) {
		f(c, client)
	}
}
var apiCmd = &cobra.Command{
	Use:   "api",
	Short: "support http api",

	RunE: func(cmd *cobra.Command, args []string) error {
		address, _ := cmd.Flags().GetString("address")
		port, _ := cmd.Flags().GetInt32("port")
		cors, _ := cmd.Flags().GetBool("cors")
		client, _ := cmd.Flags().GetString("client")
		if len(client) == 0 {
			client = "healer"
		}

		fullAddress := fmt.Sprintf("%s:%d", address, port)
		router := gin.Default()

		if cors {
			router.Use(func(c *gin.Context) {
				c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
				c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
				c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
				c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT")

				if c.Request.Method == "OPTIONS" {
					c.AbortWithStatus(204)
					return
				}

				c.Next()
			})
		}

		router.GET("/", func(c *gin.Context) {
			c.String(http.StatusOK, "")
		})

		router.GET("/health", func(c *gin.Context) {
			c.String(http.StatusOK, "")
		})

		router.GET("/metadata", apicontrollers.GetMetadata)

		router.GET("/topic/:topic/configs", wrap(apicontrollers.GetTopicConfigs, client))
		router.GET("/topic/:topic/config/:config", wrap(apicontrollers.GetTopicConfig, client))
		router.POST("/topic/:topic/config/:config/:value", wrap(apicontrollers.AlterTopicConfig, client))

		router.GET("/groups", wrap(apicontrollers.ListGroups, client))
		router.GET("/group/:group", wrap(apicontrollers.DescribeGroups, client))

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

		router.POST("/list-partition-reassignments", wrap(apicontrollers.ListPartitionReassignments, client))

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
	apiCmd.Flags().Bool("cors", false, "enable cors")

	rootCmd.AddCommand(apiCmd)
}
