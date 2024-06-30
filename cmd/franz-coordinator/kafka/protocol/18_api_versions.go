package protocol

import (
	"sort"
	"sync"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() {
	registerProtocolHandler(18, 0, 3, &APIVersions{})
}

type APIVersions struct {
	apiVersionsOnce   sync.Once
	apiVersionsSorted []kmsg.ApiVersionsResponseApiKey
}

func (h *APIVersions) Handle(kreq kmsg.Request) (kmsg.Response, error) {
	req := kreq.(*kmsg.ApiVersionsRequest)
	resp := req.ResponseKind().(*kmsg.ApiVersionsResponse)

	h.apiVersionsOnce.Do(func() {
		for _, v := range apiVersions {
			h.apiVersionsSorted = append(h.apiVersionsSorted, kmsg.ApiVersionsResponseApiKey{
				ApiKey:     v.ApiKey,
				MinVersion: v.MinVersion,
				MaxVersion: v.MaxVersion,
			})
		}
		sort.Slice(h.apiVersionsSorted, func(i, j int) bool {
			return h.apiVersionsSorted[i].ApiKey < h.apiVersionsSorted[j].ApiKey
		})
	})

	resp.ApiKeys = h.apiVersionsSorted
	return resp, nil
}
