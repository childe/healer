package cmd

import (
	"fmt"
	"net/http"

	"github.com/childe/healer/command/cmd/apicontrollers"

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
		router.GET("/topic/:topic/offsets", wrap(apicontrollers.GetTopicOffsets, client))
		router.GET("/topic/:topic/logdirs", wrap(apicontrollers.GetTopicLogDirs, client))
		router.GET("/topic/:topic/config/:config", wrap(apicontrollers.GetTopicConfig, client))
		router.POST("/topic/:topic/config/:config/:value", wrap(apicontrollers.AlterTopicConfig, client))

		router.GET("/groups", wrap(apicontrollers.ListGroups, client))
		router.GET("/group/:group", wrap(apicontrollers.DescribeGroups, client))
		router.GET("/group/:group/pending/:topic", wrap(apicontrollers.GetPending, client))

		router.POST("/alter-partition-reassignments", wrap(apicontrollers.AlterPartitionReassignments, client))

		router.POST("/list-partition-reassignments", wrap(apicontrollers.ListPartitionReassignments, client))

		router.POST("/create-partitions", wrap(apicontrollers.CreatePartitions, client))

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
