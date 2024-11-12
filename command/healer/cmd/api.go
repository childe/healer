package cmd

import (
	"net"
	"net/http"
	"strconv"

	"github.com/childe/healer/command/healer/cmd/apicontrollers"

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

		fullAddress := net.JoinHostPort(address, strconv.Itoa(int(port)))
		router := gin.Default()

		if cors {
			router.Use(func(c *gin.Context) {
				c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
				c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
				c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
				c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE")

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

		router.GET("/topic/:topic/configs", func(c *gin.Context) {
			topic := c.Param("topic")
			c.Redirect(http.StatusMovedPermanently, "/configs/topic/"+topic+"?"+c.Request.URL.RawQuery)
		})
		router.GET("/topic/:topic/offsets", wrap(apicontrollers.GetTopicOffsets, client))
		router.GET("/topic/:topic/logdirs", wrap(apicontrollers.GetTopicLogDirs, client))
		router.GET("/topic/:topic/config/:config", wrap(apicontrollers.GetTopicConfig, client))
		router.POST("/topic/:topic/config/:config/:value", wrap(apicontrollers.AlterTopicConfig, client))

		router.GET("/configs/topic/:topic", wrap(apicontrollers.GetTopicConfigs, client))
		router.GET("/configs/broker/:broker", wrap(apicontrollers.GetBrokerConfigs, client))

		router.GET("/groups", wrap(apicontrollers.ListGroups, client))
		router.GET("/group/:group", wrap(apicontrollers.DescribeGroups, client))
		router.GET("/group/:group/pending/:topic", wrap(apicontrollers.GetPending, client))

		router.POST("/alter-partition-reassignments", wrap(apicontrollers.AlterPartitionReassignments, client))
		router.POST("/elect-leaders", wrap(apicontrollers.ElectLeaders, client))

		router.POST("/list-partition-reassignments", wrap(apicontrollers.ListPartitionReassignments, client))

		router.POST("/create-partitions", wrap(apicontrollers.CreatePartitions, client))

		router.POST("/describe-logdirs", wrap(apicontrollers.DescribeLogdirs, client))

		router.POST("/describe-acls", wrap(apicontrollers.DescribeAcls, client))
		router.POST("/create-acls", wrap(apicontrollers.CreateAcls, client))
		router.DELETE("/delete-acls", wrap(apicontrollers.DeleteAcls, client))

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
