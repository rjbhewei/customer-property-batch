package common

import (
	"github.com/op/go-logging"
	"os"
)

const SEPARATOR = "_"

func GenerateId(customerno string, platform string, tenantId string) string {
	return customerno + SEPARATOR + platform + SEPARATOR + tenantId;
}

var (
	mylog = logging.MustGetLogger("main")
)

func init() {
	format := logging.MustStringFormatter(
		`[%{time:2006-01-02 15:04:05.000}] [%{level:.5s}] [%{shortfunc}:%{shortfile}] [%{callpath}] [-[%{message}]-]`,
	)
	leveledBackend := logging.AddModuleLevel(logging.NewBackendFormatter(logging.NewLogBackend(os.Stdout, "", 0), format))
	leveledBackend.SetLevel(logging.INFO, "")
	logging.SetBackend(leveledBackend)
}

func Log() *logging.Logger {
	return mylog
}

