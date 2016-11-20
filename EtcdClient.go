package main

import (
	"github.com/rjbhewei/customer-property-batch/common"
)

var mylog = common.Log()

func main() {
	mylog.Info(common.EtcdService("http://172.18.21.62:2379","/service/local/platform/qa/cryptserver/1.0"))
}
