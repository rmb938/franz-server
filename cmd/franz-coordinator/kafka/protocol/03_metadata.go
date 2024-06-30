package protocol

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"k8s.io/utils/ptr"
)

func init() {
	registerProtocolHandler(3, 0, 12, &Metadata{})
}

type Metadata struct {
}

func (h *Metadata) Handle(kreq kmsg.Request) (kmsg.Response, error) {
	req := kreq.(*kmsg.MetadataRequest)
	resp := req.ResponseKind().(*kmsg.MetadataResponse)

	sb := kmsg.NewMetadataResponseBroker()
	sb.NodeID = 1
	sb.Host = "127.0.0.1"
	sb.Port = 29092
	resp.Brokers = append(resp.Brokers, sb)

	resp.ClusterID = ptr.To("hello-world")
	resp.ControllerID = 1

	for _, rt := range req.Topics {
		st := kmsg.NewMetadataResponseTopic()
		st.TopicID = [16]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}

		if rt.TopicID != [16]byte{} {
			// TODO: going to always return unknown topic id for now
			st.TopicID = rt.TopicID
			st.ErrorCode = kerr.UnknownTopicID.Code
			resp.Topics = append(resp.Topics, st)
			continue
		} else if rt.Topic == nil {
			continue
		}

		st.Topic = rt.Topic

		// TODO: going to always return unknown topic for now
		if !req.AllowAutoTopicCreation {
			st.ErrorCode = kerr.UnknownTopicID.Code
			resp.Topics = append(resp.Topics, st)
			continue
		}

		sp := kmsg.NewMetadataResponseTopicPartition()
		sp.Partition = 1
		sp.Leader = 1
		sp.LeaderEpoch = 1
		sp.Replicas = append(sp.Replicas, 1)
		sp.ISR = sp.Replicas

		st.Partitions = append(st.Partitions, sp)
		resp.Topics = append(resp.Topics, st)
	}

	return resp, nil
}
