package servers

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/c12s/agent_queue/internal/configs"
	"github.com/c12s/agent_queue/internal/services"
	"github.com/c12s/agent_queue/pkg/api"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type agentQueueServer struct {
	api.UnimplementedAgentQueueServer
	natsConn   *nats.Conn
	authorizer services.AuthZService
}

func (s *agentQueueServer) DisseminateAppConfig(ctx context.Context, in *api.DeseminateConfigRequest) (*api.DeseminateConfigResponse, error) {
	err := s.authorizer.Authorize(ctx, "config.app.send", "node", in.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.PermissionDenied, err.Error())
	}

	log.Println("[DeseminateAppConfig]: Endpoint execution.")
	nodeId, err := uuid.Parse(in.NodeId)
	if err != nil {
		log.Printf("[DeseminateAppConfig] Error: %s is not a valid UUID", in.NodeId)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	err = s.natsConn.Publish(fmt.Sprintf("%s.app_config", &nodeId), in.Config)
	if err != nil {
		log.Printf("[DeseminateAppConfig] Error while publishing msg to nats. %v", err)
		return nil, status.Errorf(codes.Aborted, "Could not publish message to nats queue")
	}
	return &api.DeseminateConfigResponse{}, nil
}

func (s *agentQueueServer) JoinCluster(ctx context.Context, in *api.JoinClusterRequest) (*api.JoinClusterResponse, error) {
	log.Println("[JoinCluster]: Endpoint execution.")
	nodeId, err := uuid.Parse(in.NodeId)
	if err != nil {
		log.Printf("[JoinCluster] Error: %s is not a valid UUID", in.NodeId)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	err = s.natsConn.Publish(fmt.Sprintf("%s.join", &nodeId), []byte(fmt.Sprintf("%s|%s", in.JoinAddress, in.ClusterId)))
	if err != nil {
		log.Printf("[JoinCluster] Error while publishing msg to nats. %v", err)
		return nil, status.Errorf(codes.Aborted, "Could not publish message to nats queue")
	}
	return &api.JoinClusterResponse{}, nil
}

func (s *agentQueueServer) DeseminateConfig(ctx context.Context, in *api.DeseminateConfigRequest) (*api.DeseminateConfigResponse, error) {
	err := s.authorizer.Authorize(ctx, "config.send", "node", in.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.PermissionDenied, err.Error())
	}

	log.Println("[DeseminateConfig]: Endpoint execution.")
	nodeId, err := uuid.Parse(in.NodeId)
	if err != nil {
		log.Printf("[DeseminateConfig] Error: %s is not a valid UUID", in.NodeId)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	replySubject := fmt.Sprintf("%s/%s", in.NodeId, uuid.New().String())
	subsctiption, err := s.natsConn.Subscribe(replySubject, func(msg *nats.Msg) {
		if msg.Header.Get("Status") != "" {
			log.Printf("[DeseminateConfig] Skipping no-responders/status message for node %s", in.NodeId)
			return
		}
		_, err := http.Post(in.Webhook, "application/protobuf", bytes.NewBuffer(msg.Data))
		if err != nil {
			log.Printf("[DeseminateConfig] Error: Response from webhook %s: %s", in.Webhook, err.Error())
		}
	})
	if err != nil {
		log.Printf("[DeseminateConfig] Error: Could not subscribe to the reply subject %s", replySubject)
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	go func() {
		time.Sleep(10 * time.Second)
		if subsctiption.IsValid() {
			subsctiption.Unsubscribe()
		}
	}()

	err = s.natsConn.PublishRequest(fmt.Sprintf("%s.configs", &nodeId), replySubject, in.Config)
	if err != nil {
		log.Printf("[DeseminateConfig] Error while publishing config to nats. %v", err)
		return nil, status.Errorf(codes.Aborted, "Could not publish message to nats queue")
	}
	return &api.DeseminateConfigResponse{}, nil
}

func (s *agentQueueServer) DisseminateOSImage(ctx context.Context, in *api.OSImageRequest) (*api.OSImageResponse, error) {
	err := s.authorizer.Authorize(ctx, "os.update", "node", in.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.PermissionDenied, err.Error())
	}

	log.Println("[DisseminateOSImage]: Endpoint execution.")
	nodeId, err := uuid.Parse(in.NodeId)
	if err != nil {
		log.Printf("[DisseminateOSImage] Error: %s is not a valid UUID", in.NodeId)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	if in.ImageRef == "" || in.Digest == "" {
		log.Printf("[DisseminateOSImage] Error: missing image_ref or digest for node %s", in.NodeId)
		return nil, status.Errorf(codes.InvalidArgument, "image_ref and digest are required")
	}

	cmd, err := proto.Marshal(&api.OSUpdateCmd{
		ImageRef:     in.ImageRef,
		Digest:       in.Digest,
		RebootPolicy: in.RebootPolicy,
	})
	if err != nil {
		log.Printf("[DisseminateOSImage] Error: could not marshal command: %v", err)
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	replySubject := fmt.Sprintf("%s/%s", in.NodeId, uuid.New().String())
	ackChan := make(chan struct{}, 1)
	subscription, err := s.natsConn.Subscribe(replySubject, func(msg *nats.Msg) {
		if msg.Header.Get("Status") != "" {
			log.Printf("[DisseminateOSImage] Skipping no-responders/status message for node %s", in.NodeId)
			return
		}
		ackChan <- struct{}{}
	})
	if err != nil {
		log.Printf("[DisseminateOSImage] Error: could not subscribe to the reply subject %s", replySubject)
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	defer subscription.Unsubscribe()

	err = s.natsConn.PublishRequest(fmt.Sprintf("%s.os_update", &nodeId), replySubject, cmd)
	if err != nil {
		log.Printf("[DisseminateOSImage] Error while publishing command to nats. %v", err)
		return nil, status.Errorf(codes.Aborted, "Could not publish message to nats queue")
	}

	select {
	case <-ackChan:
		log.Printf("[DisseminateOSImage] Node %s acknowledged the os update command", in.NodeId)
		return &api.OSImageResponse{Accepted: true}, nil
	case <-time.After(10 * time.Second):
		log.Printf("[DisseminateOSImage] Node %s did not acknowledge the os update command", in.NodeId)
		return &api.OSImageResponse{Accepted: false}, nil
	case <-ctx.Done():
		log.Printf("[DisseminateOSImage] Context done while waiting for node %s ack", in.NodeId)
		return &api.OSImageResponse{Accepted: false}, nil
	}
}

func GetAuthInterceptor() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if ok && len(md.Get("authz-token")) > 0 {
			ctx = context.WithValue(ctx, "authz-token", md.Get("authz-token")[0])
		}
		return handler(ctx, req)
	}
}

func Serve(natsConn *nats.Conn, grpcPort int) {
	log.Println("Starting grpc server...")
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		panic(err)
	}

	//server := grpc.NewServer()
	server := grpc.NewServer(grpc.UnaryInterceptor(GetAuthInterceptor()))
	config, _ := configs.NewFromEnv()
	authorizer := services.NewAuthZService(config.TokenKey())
	api.RegisterAgentQueueServer(server, &agentQueueServer{natsConn: natsConn, authorizer: authorizer})
	reflection.Register(server)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("Failed to start grpc server: %v", err)
	}
}
