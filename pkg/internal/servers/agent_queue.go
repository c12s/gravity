package servers

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/c12s/agent_queue/pkg/api"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type agentQueueServer struct {
	api.UnimplementedAgentQueueServer
	natsConn *nats.Conn
}

func (s *agentQueueServer) DeseminateConfig(ctx context.Context, in *api.DeseminateConfigRequest) (*api.DeseminateConfigResponse, error) {
	log.Println("[DeseminateConfig]: Endpoint execution.")
	nodeId, err := uuid.Parse(in.NodeId)
	if err != nil {
		log.Printf("[DeseminateConfig] Error: %s is not a valid UUID", in.NodeId)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	err = s.natsConn.Publish(fmt.Sprintf("%s.configs", &nodeId), in.Config)
	if err != nil {
		log.Printf("[DeseminateConfig] Error while publishing config to nats. %v", err)
		return nil, status.Errorf(codes.Aborted, "Could not publish message to nats queue")
	}
	return &api.DeseminateConfigResponse{}, nil
}

func Serve(natsConn *nats.Conn, grpcPort int) {
	log.Println("Starting grpc server...")
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		panic(err)
	}

	server := grpc.NewServer()
	api.RegisterAgentQueueServer(server, &agentQueueServer{natsConn: natsConn})
	reflection.Register(server)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("Failed to start grpc server: %v", err)
	}
}
