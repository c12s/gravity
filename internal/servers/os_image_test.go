package servers

import (
	"strings"
	"testing"

	"github.com/c12s/agent_queue/pkg/api"
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
