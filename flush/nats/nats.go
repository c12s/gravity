package nats

import (
	"context"
	"fmt"
	gPb "github.com/c12s/scheme/gravity"
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

func (f *Flusher) Flush(ctx context.Context, data *gPb.FlushTask) {
	fmt.Println("Flush")
	for _, val := range data.Parts {
		fmt.Println(val.Nodes)
	}
}
