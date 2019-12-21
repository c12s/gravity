package service

import (
	"errors"
	"fmt"
	"github.com/c12s/gravity/config"
	"github.com/c12s/gravity/storage"
	bPb "github.com/c12s/scheme/blackhole"
	gPb "github.com/c12s/scheme/gravity"
	sg "github.com/c12s/stellar-go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)

type Server struct {
	db         storage.DB
	instrument map[string]string
}

func (s *Server) atOnce(ctx context.Context, req *gPb.PutReq) (*gPb.PutResp, error) {
	span, _ := sg.FromGRPCContext(ctx, "atOnce")
	defer span.Finish()
	fmt.Println(span)

	// Chop those results based on strategy kind and interval
	// Store into db jobs defined by strategy parameter
	m := req.Task.Mutate
	task := m.Task
	kind := m.Kind.String()
	err := s.db.Chop(sg.NewTracedGRPCContext(ctx, span), task.Strategy, req.Task.Index, req.Key, kind, req.TaskKey, task.Payload)
	if err != nil {
		span.AddLog(&sg.KV{"chop error", err.Error()})
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
	span, _ := sg.FromGRPCContext(ctx, "gravity.PutTask")
	defer span.Finish()
	fmt.Println(span)
	fmt.Println("SERIALIZE ", span.Serialize())

	putTask := req.Task.Mutate.Task
	switch putTask.Strategy.Type {
	case bPb.StrategyKind_AT_ONCE:
		return s.atOnce(sg.NewTracedGRPCContext(ctx, span), req)
	case bPb.StrategyKind_ROLLING_UPDATE:
		return s.rollingUpdate(sg.NewTracedGRPCContext(ctx, span), req)
	case bPb.StrategyKind_CANARY:
		return s.canary(sg.NewTracedGRPCContext(ctx, span), req)
	}

	span.AddLog(&sg.KV{"kind error", "Uknown update type!"})
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
		db:         db,
		instrument: conf.InstrumentConf,
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n, err := sg.NewCollector(gravityServer.instrument["address"], gravityServer.instrument["stopic"])
	if err != nil {
		fmt.Println(err)
		return
	}
	c, err := sg.InitCollector(gravityServer.instrument["location"], n)
	if err != nil {
		fmt.Println(err)
		return
	}
	go c.Start(ctx, 15*time.Second)
	db.StartControllers(ctx)

	fmt.Println("Gravity RPC Started")
	gPb.RegisterGravityServiceServer(server, gravityServer)
	server.Serve(lis)
}
