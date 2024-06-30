package main

import (
	"fmt"
	"net"
	"os"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/rmb938/franz-server/cmd/franz-coordinator/kafka/client"
	"go.uber.org/zap"
)

var mainLog logr.Logger

func main() {

	zapLog, err := zap.NewDevelopment()
	if err != nil {
		panic(fmt.Sprintf("error creating zap logger: %s", err))
	}
	mainLog = zapr.NewLogger(zapLog)

	listenerAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:29092")
	if err != nil {
		mainLog.Error(err, "error parsing listener address")
		os.Exit(1)
	}

	listener, err := net.Listen("tcp", listenerAddr.String())
	if err != nil {
		mainLog.Error(err, "error creating listener")
		os.Exit(1)
	}

	mainLog.Info("Starting Listener", "address", listenerAddr.String())

	for {
		conn, err := listener.Accept()
		if err != nil {
			mainLog.Error(err, "error accepting client connection")
			os.Exit(1)
		}

		kc := client.NewClient(mainLog, conn)
		go kc.Run()
	}

}
