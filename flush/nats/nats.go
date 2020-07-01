package nats

import (
	"context"
	"fmt"
	h "github.com/c12s/gravity/storage/etcd"
	fPb "github.com/c12s/scheme/flusher"
	gPb "github.com/c12s/scheme/gravity"
	sPb "github.com/c12s/scheme/stellar"
	sg "github.com/c12s/stellar-go"
	"github.com/golang/protobuf/proto"
	nats "github.com/nats-io/nats.go"
	"strings"
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

func parse(tags string) map[string]string {
	rez := map[string]string{}
	for _, item := range strings.Split(tags, ";") {
		pair := strings.Split(item, ":")
		rez[pair[0]] = pair[1]
	}

	return rez
}

func (f *Flusher) Flush(ctx context.Context, data *gPb.FlushTask) {
	span, _ := sg.FromContext(ctx, "flusher.flush")
	defer span.Finish()
	fmt.Println(span)

	for _, part := range data.Parts {
		for _, node := range part.Nodes {
			ssp := span.Serialize()
			state, err := proto.Marshal(&fPb.Event{
				Payload: data.Payload,
				TaskKey: data.TaskKey,
				Kind:    h.GKind(node),
				SpanContext: &sPb.SpanContext{
					TraceId:       ssp.Get("trace_id")[0],
					SpanId:        ssp.Get("span_id")[0],
					ParrentSpanId: ssp.Get("parrent_span_id")[0],
					Baggage:       parse(ssp.Get("tags")[0]),
				},
			})
			if err != nil {
				span.AddLog(&sg.KV{"marshaling error", err.Error()})
				fmt.Println("{{GRAVITY ERROR}}", err.Error())
				continue
			}

			span.AddLog(&sg.KV{"pushing to key", h.TransformKey(node)})
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
