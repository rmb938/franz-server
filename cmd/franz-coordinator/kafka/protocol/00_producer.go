package protocol

import (
	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() {
	registerProtocolHandler(0, 3, 10, &Produce{})
}

type Produce struct {
}

func (h *Produce) Handle(kreq kmsg.Request) (kmsg.Response, error) {
	req := kreq.(*kmsg.ProduceRequest)
	resp := req.ResponseKind().(*kmsg.ProduceResponse)

	// TODO: no transaction support right now
	if req.TransactionID != nil {
		// TODO: error about transaction id auth failed
	}

	switch req.Acks {
	case -1, 0, 1:
	default:
		// TODO: error about invalid required acks
	}

	return resp, nil
}
