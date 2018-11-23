package nats

import (
	"context"
	bPb "github.com/c12s/scheme/blackhole"
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

func (f *Flusher) Flush(ctx context.Context, key string, payloads []*bPb.Payload) error {
	return nil
}
