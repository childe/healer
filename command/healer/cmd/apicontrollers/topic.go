package apicontrollers

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
)

// GetTopicConfig godoc
// @Summary      获取主题特定配置
// @Description  获取指定主题的特定配置项
// @Tags         topics
// @Accept       json
// @Produce      json
// @Param        topic      path    string  true   "主题名称"
// @Param        config     path    string  true   "配置项名称"
// @Param        bootstrap  query   string  true   "Kafka bootstrap servers, 格式: host1:port1,host2:port2"
// @Success      200       {object}  map[string]interface{}
// @Router       /topic/{topic}/config/{config} [get]
func GetTopicConfig(c *gin.Context, client string) {
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
		c.String(http.StatusInternalServerError, err.Error())
	}
	resp, err := controller.RequestAndGet(r)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	} else {
		c.JSON(http.StatusOK, resp)
	}
}

// AlterTopicConfig godoc
// @Summary      修改主题配置
// @Description  修改指定主题的特定配置项值
// @Tags         topics
// @Accept       json
// @Produce      json
// @Param        topic      path    string  true   "主题名称"
// @Param        config     path    string  true   "配置项名称"
// @Param        value      path    string  true   "配置项值"
// @Param        bootstrap  query   string  true   "Kafka bootstrap servers, 格式: host1:port1,host2:port2"
// @Success      200       {object}  map[string]interface{}
// @Router       /topic/{topic}/config/{config}/{value} [post]
func AlterTopicConfig(c *gin.Context, client string) {
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
		c.String(http.StatusInternalServerError, err.Error())
	}
	resp, err := controller.RequestAndGet(r)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	} else {
		c.JSON(http.StatusOK, resp)
	}
}

// GetTopicOffsets godoc
// @Summary      获取主题偏移量
// @Description  获取指定主题的偏移量信息
// @Tags         topics
// @Accept       json
// @Produce      json
// @Param        topic      path    string  true   "主题名称"
// @Param        bootstrap  query   string  true   "Kafka bootstrap servers, 格式: host1:port1,host2:port2"
// @Param        timestamp  query   string  false  "时间戳（毫秒）"
// @Success      200       {object}  map[string]interface{}
// @Router       /topic/{topic}/offsets [get]
func GetTopicOffsets(c *gin.Context, client string) {
	bootstrapServers := c.Query("bootstrap")
	bs, err := healer.NewBrokers(bootstrapServers)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	defer bs.Close()

	timestamp := c.Query("timestamp")
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("timestamp value error: %s", err))
		return
	}

	topic := c.Param("topic")

	rst := make([]healer.PartitionOffset, 0)

	offsetsResponse, err := bs.RequestOffsets(client, topic, -1, ts, 1)

	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	for _, x := range offsetsResponse {
		for _, partitionOffsetsList := range x.TopicPartitionOffsets {
			rst = append(rst, partitionOffsetsList...)
		}
	}

	sort.Slice(rst, func(i, j int) bool { return rst[i].Partition < rst[j].Partition })
	c.JSON(http.StatusOK, rst)
}

// GetTopicLogDirs godoc
// @Summary      获取主题日志目录
// @Description  获取指定主题的日志目录信息
// @Tags         topics
// @Accept       json
// @Produce      json
// @Param        topic      path    string  true   "主题名称"
// @Param        bootstrap  query   string  true   "Kafka bootstrap servers, 格式: host1:port1,host2:port2"
// @Success      200       {object}  map[string]interface{}
// @Router       /topic/{topic}/logdirs [get]
func GetTopicLogDirs(c *gin.Context, clientID string) {
	bootstrapServers := c.Query("bootstrap")
	client, err := healer.NewClient(bootstrapServers, clientID)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	defer client.Close()

	topic := c.Param("topic")

	rst, err := client.DescribeLogDirs([]string{topic})

	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, rst)
}
