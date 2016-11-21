package main

import (
	"github.com/rjbhewei/customer-property-batch/common"
	"golang.org/x/net/context"
	pb "github.com/rjbhewei/customer-property-batch/helloworld"
	"net"
	"google.golang.org/grpc"
)

var (
	mylog = common.Log()
)

const (
	port = ":8888"
)

type server struct{}

func (s *server) ToEncrypt(ctx context.Context, in *pb.EncryptRequest) (*pb.EncryptReply, error) {
	return &pb.EncryptReply{
		Customers: in.Customers,
		Platform: in.Platform + "<-",
		TenantId: in.TenantId + "<-",
		Value: in.Value + "<-",
		PropertyId: in.PropertyId + "<-",
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", port)

	if err != nil {
		mylog.Errorf("failed to listen: %v", err)
	}

	s := grpc.NewServer()

	pb.RegisterEncryptServer(s, &server{})

	if err := s.Serve(lis); err != nil {
		mylog.Errorf("failed to serve: %v", err)
	}
}
