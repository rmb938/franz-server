package main

import (
	"fmt"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
)

var mainLog logr.Logger

func main() {

	zapLog, err := zap.NewDevelopment()
	if err != nil {
		panic(fmt.Sprintf("error creating zap logger: %s", err))
	}
	mainLog = zapr.NewLogger(zapLog)

	mainLog.Info("Hello World! from data server")

}
