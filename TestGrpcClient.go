package main

import (
	"github.com/rjbhewei/customer-property-batch/common"
	"encoding/json"
	"google.golang.org/grpc"
	pb "github.com/rjbhewei/customer-property-batch/helloworld"
	"golang.org/x/net/context"
)

var (
	mylog = common.Log()
	bean = &common.BatchUpdateBean{
		Customers: []string{"Customers1", "Customers2"},
		Platform :"Platform",
		TenantId : "TenantId",
		Value :"Value",
		PropertyId:"PropertyId",
	}
)

const (
	address = "localhost:50051"
)

func init() {
	j, _ := json.Marshal(bean)
	mylog.Info(string(j))
}

func main() {
	conn, err := grpc.Dial(address, grpc.WithInsecure())

	if err != nil {
		mylog.Errorf("did not connect: %v", err)
	}

	defer conn.Close()

	c := pb.NewEncryptClient(conn)

	r, err := c.ToEncrypt(context.Background(), &pb.EncryptRequest{
		Customers: bean.Customers,
		Platform: bean.Platform,
		TenantId: bean.TenantId,
		Value: bean.Value,
		PropertyId: bean.PropertyId,
	})

	if err != nil {
		mylog.Errorf("rpc encrypt error: %v", err)
		return
	}

	j, _ := json.Marshal(r)

	mylog.Infof("EncryptReply : %s", string(j))
}

