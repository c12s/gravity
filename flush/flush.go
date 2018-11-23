package flush

import (
	"context"
)

type Flusher interface {
	Flush(ctx context.Context, key string, data map[string]string) error
}
