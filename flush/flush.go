package flush

import (
	"context"
	bPb "github.com/c12s/scheme/blackhole"
)

type Flusher interface {
	Flush(ctx context.Context, key string, payloads []*bPb.Payload) error
}
