package nats

import (
	"context"
	"fmt"
	h "github.com/c12s/gravity/storage/etcd"
	fPb "github.com/c12s/scheme/flusher"
	gPb "github.com/c12s/scheme/gravity"
	"github.com/golang/protobuf/proto"
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
	for _, part := range data.Parts {
		for _, node := range part.Nodes {
			state, err := proto.Marshal(&fPb.Event{
				Payload: data.Payload,
				TaskKey: data.TaskKey,
				Kind:    h.Kind(node),
			})
			if err != nil {
				//TODO: Add to logging service an entry about fail
				continue
			}

			//TODO: Should be added to the logging service
			fmt.Print("Pusing to key: ")
			fmt.Println(h.TransformKey(node))
			f.nc.Publish(h.TransformKey(node), state)
		}
	}
}

func (fl *Flusher) Sub(topic string, f func(u *fPb.Update)) {
	fl.nc.Subscribe(topic, func(msg *nats.Msg) {
		data := &fPb.Update{}
		err := proto.Unmarshal(msg.Data, data)
		if err != nil {
			f(nil)
		}
		f(data)
	})
	fl.nc.Flush()
}
