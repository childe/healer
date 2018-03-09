package healer

import "github.com/golang/glog"

type MetaInfo struct {
	brokers        []*BrokerInfo
	topicMetadatas []*TopicMetadata
	//groups         []string
}

type Helper struct {
	clientID string
	brokers  *Brokers
	metaInfo *MetaInfo
}

var DEFAULT_CLIENT_ID string = "HEALERHELPER"

func NewHelper(brokerList string, config map[string]interface{}) (*Helper, error) {
	//func NewBrokers(brokerList string, clientID string, connecTimeout int, timeout int) (*Brokers, error) {
	var (
		connecTimeout int = 60000
		timeout       int = 60000
		err           error
	)
	h := &Helper{}

	if v, ok := config["clientID"]; ok {
		h.clientID = v.(string)
	} else {
		h.clientID = DEFAULT_CLIENT_ID
	}

	h.brokers, err = NewBrokers(brokerList, h.clientID, connecTimeout, timeout)
	h.metaInfo = &MetaInfo{}
	if err != nil {
		return nil, err
	}

	return h, nil
}

func (h *Helper) UpdateMeta() error {
	metadataResponse, err := h.brokers.RequestMetaData(h.clientID, []string{})
	if err != nil {
		return err
	}

	h.metaInfo = &MetaInfo{}
	h.metaInfo.brokers = metadataResponse.Brokers
	h.metaInfo.topicMetadatas = metadataResponse.TopicMetadatas

	return nil
}

func (h *Helper) GetGroups() []string {
	//if h.metaInfo.groups != nil {
	//return h.metaInfo.groups
	//}

	//if h.metaInfo.brokers == nil {
	//err := h.UpdateMeta()
	//if err != nil {
	//glog.Errorf("update meta error:%s", err)
	//return nil
	//}
	//}

	err := h.UpdateMeta()
	if err != nil {
		glog.Errorf("update meta error:%s", err)
		return nil
	}
	groups := []string{}
	for _, brokerinfo := range h.metaInfo.brokers {
		broker, err := h.brokers.GetBroker(brokerinfo.NodeId)
		if err != nil {
			glog.Errorf("get broker [%d] error:%s", brokerinfo.NodeId, err)
			return nil
		}

		response, err := broker.requestListGroups(h.clientID)
		if err != nil {
			glog.Errorf("get group list from broker[%s] error:%s", broker.GetAddress(), err)
			return nil
		}
		for _, g := range response.Groups {
			groups = append(groups, g.GroupID)
		}
	}

	return groups
}
