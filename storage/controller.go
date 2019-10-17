package storage

import (
	"context"
)

type ControllManager interface {
	Start(ctx context.Context)
}
