package flush

import (
	"context"
	fPb "github.com/c12s/scheme/flusher"
	gPb "github.com/c12s/scheme/gravity"
)

type Flusher interface {
	Flush(ctx context.Context, payloads *gPb.FlushTask)
	Sub(topic string, f func(msg *fPb.Update))
}
