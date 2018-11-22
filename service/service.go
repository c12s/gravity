package service

import (
	"fmt"
	gPb "github.com/c12s/scheme/gravity"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
)

type Server struct {
}

func (s *Server) PutTask(ctx context.Context, req *gPb.PutReq) (*gPb.PutResp, error) {
	return nil, nil
}

func Run(address string) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to initializa TCP listen: %v", err)
	}
	defer lis.Close()

	server := grpc.NewServer()
	gravityServer := &Server{
		db: db,
	}

	fmt.Println("Gravity RPC Started")
	gPb.RegisterGravityServiceServer(server, gravityServer)
	server.Serve(lis)
}
