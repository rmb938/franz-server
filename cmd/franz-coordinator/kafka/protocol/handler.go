package protocol

import (
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kmsg"
)

var (
	apiVersionsLock sync.Mutex
	apiVersions     = make(map[int16]APIVersionsProtocolHandler)
)

type ProtocolHandler interface {
	Handle(kreq kmsg.Request) (kmsg.Response, error)
}

type APIVersionsProtocolHandler struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16

	Handler ProtocolHandler
}

func registerProtocolHandler(key, min, max int16, handler ProtocolHandler) {
	apiVersionsLock.Lock()
	defer apiVersionsLock.Unlock()

	if key < 0 || min < 0 || max < 0 || max < min {
		panic(fmt.Sprintf("invalid api key registered, key: %d, min: %d, max: %d", key, min, max))
	}
	if _, exists := apiVersions[key]; exists {
		panic(fmt.Sprintf("api key registered multiple times %d", key))
	}

	apiVersions[key] = APIVersionsProtocolHandler{
		ApiKey:     key,
		MinVersion: min,
		MaxVersion: max,

		Handler: handler,
	}
}

func checkRequestVersion(key, version int16) error {
	apiKey, exists := apiVersions[key]
	if !exists {
		return fmt.Errorf("unsupported request key %d", key)
	}

	if version < apiKey.MinVersion {
		return fmt.Errorf("%s version %d below min supported version %d", kmsg.NameForKey(key), version, apiKey.MinVersion)
	}

	if version > apiKey.MaxVersion {
		return fmt.Errorf("%s version %d above max supported version %d", kmsg.NameForKey(key), version, apiKey.MaxVersion)
	}
	return nil
}

func HandleProtocolPacket(kreq kmsg.Request) (kmsg.Response, error) {
	err := checkRequestVersion(kreq.Key(), kreq.GetVersion())
	if err != nil {
		return nil, err
	}

	apiVersionProtocolHandler, exists := apiVersions[kreq.Key()]
	if !exists {
		return nil, fmt.Errorf("cannot handle unknown api key: %s", kmsg.NameForKey(kreq.Key()))
	}

	kresp, err := apiVersionProtocolHandler.Handler.Handle(kreq)
	if err != nil {
		return nil, fmt.Errorf("error handling api key: %s: %w", kmsg.NameForKey(kreq.Key()), err)
	}

	return kresp, nil
}
