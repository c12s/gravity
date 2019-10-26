package service

import (
	"errors"
	"fmt"
	"github.com/c12s/gravity/config"
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
}

func (s *Server) atOnce(ctx context.Context, req *gPb.PutReq) (*gPb.PutResp, error) {
	fmt.Println("atOnce")
	// Chop those results based on strategy kind and interval
	// Store into db jobs defined by strategy parameter
	m := req.Task.Mutate
	task := m.Task
	kind := m.Kind.String()
	err := s.db.Chop(ctx, task.Strategy, req.Task.Index, req.Key, kind, task.Payload)
	if err != nil {
		return nil, err
	}

	return &gPb.PutResp{}, nil
}

func (s *Server) rollingUpdate(ctx context.Context, req *gPb.PutReq) (*gPb.PutResp, error) {
	return &gPb.PutResp{}, nil
}

func (s *Server) canary(ctx context.Context, req *gPb.PutReq) (*gPb.PutResp, error) {
	return &gPb.PutResp{}, nil
}

func (s *Server) PutTask(ctx context.Context, req *gPb.PutReq) (*gPb.PutResp, error) {
	fmt.Println("PutTask")

	putTask := req.Task.Mutate.Task
	switch putTask.Strategy.Type {
	case bPb.StrategyKind_AT_ONCE:
		return s.atOnce(ctx, req)
	case bPb.StrategyKind_ROLLING_UPDATE:
		return s.rollingUpdate(ctx, req)
	case bPb.StrategyKind_CANARY:
		return s.canary(ctx, req)
	}
	return nil, errors.New("Uknown update type!")
}

func Run(conf *config.Config, db storage.DB) {
	lis, err := net.Listen("tcp", conf.Address)
	if err != nil {
		log.Fatalf("failed to initializa TCP listen: %v", err)
	}
	defer lis.Close()

	server := grpc.NewServer()
	gravityServer := &Server{
		db: db,
	}
	defer db.Close()

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	db.StartControllers(ctx)

	fmt.Println("Gravity RPC Started")
	gPb.RegisterGravityServiceServer(server, gravityServer)
	server.Serve(lis)
	cancel()
}
