package main

import (
	"github.com/rjbhewei/customer-property-batch/common"
)

var mylog = common.Log()

func main() {
	mylog.Info(common.ConsulService("10.117.8.138:8500","/cryptserver/1.0"))
}
