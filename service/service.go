package service

import (
	"fmt"
	"github.com/c12s/gravity/config"
	"github.com/c12s/gravity/flush"
	"github.com/c12s/gravity/storage"
	bPb "github.com/c12s/scheme/blackhole"
	gPb "github.com/c12s/scheme/gravity"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
)

type Server struct {
	db storage.DB
	f  flush.Flusher
}

func (s *Server) PutTask(ctx context.Context, req *gPb.PutReq) (*gPb.PutResp, error) {
	putTask := req.Task.Mutate.Task
	switch putTask.Strategy.Type {
	case bPb.StrategyKind_AT_ONCE:
	case bPb.StrategyKind_ROLLING_UPDATE:
	default:
	}

	return nil, nil
}

func Run(conf *config.Config, db storage.DB, f flush.Flusher) {
	lis, err := net.Listen("tcp", conf.Address)
	if err != nil {
		log.Fatalf("failed to initializa TCP listen: %v", err)
	}
	defer lis.Close()

	server := grpc.NewServer()
	gravityServer := &Server{
		db: db,
		f:  f,
	}

	fmt.Println("Gravity RPC Started")
	gPb.RegisterGravityServiceServer(server, gravityServer)
	server.Serve(lis)
}
