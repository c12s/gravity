package storage

import (
	"context"
)

type DB interface {
	Update(ctx context.Context, key, data string) error
	Chop(ctx context.Context, labels map[string]string) error
	Close()
}
