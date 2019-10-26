package flush

import (
	"context"
	gPb "github.com/c12s/scheme/gravity"
)

type Flusher interface {
	Flush(ctx context.Context, payloads *gPb.FlushTask)
}
