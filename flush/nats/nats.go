package nats

import (
	"context"
	"github.com/nats-io/go-nats"
)

type Flusher struct {
	nc *nats.Conn
}

func New(address string) (*Flusher, error) {
	nc, err := nats.Connect(address)
	if err != nil {
		return nil, err
	}

	return &Flusher{
		nc: nc,
	}, nil
}

func (f *Flusher) Flush(ctx context.Context, key string, data map[string]string) error {
	return nil
}
