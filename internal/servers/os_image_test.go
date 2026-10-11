package servers

import (
	"context"
	"strings"
	"testing"

	"github.com/c12s/agent_queue/pkg/api"
	magnetarapi "github.com/c12s/magnetar/pkg/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestValidateOSImage(t *testing.T) {
	good := "sha256:" + strings.Repeat("a", 64)
	ref := "192.168.122.1:5001/os/edge-base:v3.10"
	cases := []struct {
		name    string
		req     *api.OSImageRequest
		wantErr bool
	}{
		{"valid", &api.OSImageRequest{ImageRef: ref, Digest: good}, false},
		{"valid immediate", &api.OSImageRequest{ImageRef: ref, Digest: good, RebootPolicy: "immediate"}, false},
		{"missing digest", &api.OSImageRequest{ImageRef: ref}, true},
		{"short digest", &api.OSImageRequest{ImageRef: ref, Digest: "sha256:abc"}, true},
		{"uppercase digest", &api.OSImageRequest{ImageRef: ref, Digest: "sha256:" + strings.Repeat("A", 64)}, true},
		{"ref with digest", &api.OSImageRequest{ImageRef: ref + "@" + good, Digest: good}, true},
		{"unsupported policy", &api.OSImageRequest{ImageRef: ref, Digest: good, RebootPolicy: "maintenance-window"}, true},
	}
	for _, c := range cases {
		if err := validateOSImage(c.req); (err != nil) != c.wantErr {
			t.Errorf("%s: err=%v, wantErr=%v", c.name, err, c.wantErr)
		}
	}
}

type fakeOSIntent struct {
	got *magnetarapi.DesiredOsImageSetReq
	md  metadata.MD
}

func (f *fakeOSIntent) SetDesiredOsImage(ctx context.Context, in *magnetarapi.DesiredOsImageSetReq, _ ...grpc.CallOption) (*magnetarapi.DesiredOsImageSetResp, error) {
	f.got = in
	f.md, _ = metadata.FromOutgoingContext(ctx)
	return &magnetarapi.DesiredOsImageSetResp{}, nil
}

func TestRecordOSIntent(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authz-token", "tok"))

	fake := &fakeOSIntent{}
	s := &agentQueueServer{osIntent: fake}
	update := &api.OSImageRequest{NodeId: "n1", ImageRef: "reg/os:v1", Digest: digest, RebootPolicy: "immediate"}
	if err := s.recordOSIntent(ctx, update); err != nil {
		t.Fatal(err)
	}
	img := fake.got.Image
	if fake.got.NodeId != "n1" || img == nil || img.ImageRef != "reg/os:v1" || img.Digest != digest || img.RebootPolicy != "immediate" {
		t.Errorf("update recorded as %+v", fake.got)
	}
	if got := fake.md.Get("authz-token"); len(got) != 1 || got[0] != "tok" {
		t.Errorf("caller token not forwarded to magnetar: %v", fake.md)
	}

	if err := s.recordOSIntent(ctx, &api.OSImageRequest{NodeId: "n1", Rollback: true}); err != nil {
		t.Fatal(err)
	}
	if fake.got.Image != nil {
		t.Errorf("rollback must clear the declared image, got %+v", fake.got.Image)
	}
}
