package service

import (
	"errors"
	"fmt"
	"github.com/c12s/gravity/config"
	"github.com/c12s/gravity/helper"
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
	apollo     string
	meridian   string
}

func (s *Server) atOnce(ctx context.Context, req *gPb.PutReq) (*gPb.PutResp, error) {
	span, _ := sg.FromGRPCContext(ctx, "atOnce")
	defer span.Finish()
	fmt.Println(span)

	token, err := helper.ExtractToken(ctx)
	if err != nil {
		span.AddLog(&sg.KV{"token error", err.Error()})
		return nil, err
	}

	// Chop those results based on strategy kind and interval
	// Store into db jobs defined by strategy parameter
	m := req.Task.Mutate
	task := m.Task
	kind := m.Kind.String()
	err = s.db.Chop(
		helper.AppendToken(
			sg.NewTracedGRPCContext(ctx, span),
			token,
		),
		task.Strategy, req.Task.Index, req.Key,
		kind, req.TaskKey,
		task.Payload,
	)
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

	token, err := helper.ExtractToken(ctx)
	if err != nil {
		span.AddLog(&sg.KV{"token error", err.Error()})
		return nil, err
	}

	err = s.auth(ctx, putOpt(req, token))
	if err != nil {
		span.AddLog(&sg.KV{"auth error", err.Error()})
		return nil, err
	}

	_, err = s.checkNS(ctx, req.Task.Mutate.UserId, req.Task.Mutate.Namespace)
	if err != nil {
		span.AddLog(&sg.KV{"check ns error", err.Error()})
		return nil, err
	}

	putTask := req.Task.Mutate.Task
	switch putTask.Strategy.Type {
	case bPb.StrategyKind_AT_ONCE:
		return s.atOnce(
			helper.AppendToken(
				sg.NewTracedGRPCContext(ctx, span),
				token,
			),
			req,
		)
	case bPb.StrategyKind_ROLLING_UPDATE:
		return s.rollingUpdate(
			helper.AppendToken(
				sg.NewTracedGRPCContext(ctx, span),
				token,
			),
			req,
		)
	case bPb.StrategyKind_CANARY:
		return s.canary(
			helper.AppendToken(
				sg.NewTracedGRPCContext(ctx, span),
				token,
			),
			req,
		)
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
		apollo:     conf.Apollo,
		meridian:   conf.Meridian,
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
