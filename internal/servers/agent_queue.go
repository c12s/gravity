package servers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/c12s/agent_queue/internal/configs"
	"github.com/c12s/agent_queue/internal/services"
	"github.com/c12s/agent_queue/pkg/api"
	magnetarapi "github.com/c12s/magnetar/pkg/api"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type agentQueueServer struct {
	api.UnimplementedAgentQueueServer
	natsConn   *nats.Conn
	authorizer services.AuthZService
	osIntent   osIntentRecorder
}

// osIntentRecorder is the part of Magnetar's API gravity needs: the declared
// OS image per node, which nodes pull (magnetar.os.desired.get).
type osIntentRecorder interface {
	SetDesiredOsImage(ctx context.Context, in *magnetarapi.DesiredOsImageSetReq, opts ...grpc.CallOption) (*magnetarapi.DesiredOsImageSetResp, error)
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
	// a per-node grant, or an org-level grant that Magnetar matches against
	// the node's org when the intent is recorded below (review R21)
	err := s.authorizer.AuthorizeNodeOrAnyOrg(ctx, "os.update", in.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.PermissionDenied, err.Error())
	}

	log.Println("[DisseminateOSImage]: Endpoint execution.")
	nodeId, err := uuid.Parse(in.NodeId)
	if err != nil {
		log.Printf("[DisseminateOSImage] Error: %s is not a valid UUID", in.NodeId)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	if !in.Rollback {
		if err := validateOSImage(in); err != nil {
			log.Printf("[DisseminateOSImage] Error: invalid request for node %s: %v", in.NodeId, err)
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	// Intent first: once Magnetar holds it, a node that misses the push below
	// (offline, rebooting) still converges when it next pulls.
	if err := s.recordOSIntent(ctx, in); err != nil {
		log.Printf("[DisseminateOSImage] Error: could not record desired image for node %s: %v", in.NodeId, err)
		if status.Code(err) == codes.PermissionDenied || status.Code(err) == codes.NotFound {
			return nil, err
		}
		return nil, status.Errorf(codes.Unavailable, "could not record desired image: %v", err)
	}

	cmd, err := proto.Marshal(&api.OSUpdateCmd{
		ImageRef:     in.ImageRef,
		Digest:       in.Digest,
		RebootPolicy: in.RebootPolicy,
		Rollback:     in.Rollback,
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

// recordOSIntent stores the request as the node's declared image in Magnetar.
// A rollback clears it: the node returns to its previous deployment and stays
// unmanaged until the next update, so the pull must not switch it back.
func (s *agentQueueServer) recordOSIntent(ctx context.Context, in *api.OSImageRequest) error {
	req := &magnetarapi.DesiredOsImageSetReq{NodeId: in.NodeId}
	if !in.Rollback {
		req.Image = &magnetarapi.DesiredOsImage{
			ImageRef:     in.ImageRef,
			Digest:       in.Digest,
			RebootPolicy: in.RebootPolicy,
		}
	}
	// Magnetar authorizes with the caller's token, like the check above
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := s.osIntent.SetDesiredOsImage(ctx, req)
	return err
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
	magnetarConn, err := grpc.Dial(config.MagnetarAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to create magnetar client: %v", err)
	}
	api.RegisterAgentQueueServer(server, &agentQueueServer{
		natsConn:   natsConn,
		authorizer: authorizer,
		osIntent:   magnetarapi.NewMagnetarClient(magnetarConn),
	})
	reflection.Register(server)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("Failed to start grpc server: %v", err)
	}
}

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// validateOSImage rejects specs a node could never apply, before they are
// acknowledged: otherwise the node accepts them and retries a failing switch
// forever (malformed digest) or stages without rebooting (unknown policy).
func validateOSImage(in *api.OSImageRequest) error {
	switch {
	case in.ImageRef == "" || in.Digest == "":
		return errors.New("image_ref and digest are required")
	case strings.ContainsAny(in.ImageRef, "@ \t\n"):
		return fmt.Errorf("image_ref %q must be a plain reference without a digest; pass the digest separately", in.ImageRef)
	case !digestPattern.MatchString(in.Digest):
		return fmt.Errorf("digest %q must be sha256:<64 lowercase hex>", in.Digest)
	case in.RebootPolicy != "" && in.RebootPolicy != "immediate":
		return fmt.Errorf("reboot_policy %q is not supported (only \"immediate\")", in.RebootPolicy)
	}
	return nil
}
