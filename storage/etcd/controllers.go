package etcd

import (
	"context"
	"fmt"
	flusher "github.com/c12s/gravity/flush"
	fPb "github.com/c12s/scheme/flusher"
	gPb "github.com/c12s/scheme/gravity"
	sg "github.com/c12s/stellar-go"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
)

type SyncManager struct {
	db      *DB
	flusher flusher.Flusher
	topic   string
}

func (sm *SyncManager) Start(ctx context.Context) {
	sm.flusher.Sub(sm.topic, func(msg *fPb.Update) {
		go func(data *fPb.Update) {
			span, _ := sg.FromCustomSource(
				data.SpanContext,
				data.SpanContext.Baggage,
				"syncmanager.update",
			)
			fmt.Println(span)
			defer span.Finish()

			//TODO: remove task from gravity or update
			//TODO: update node job status celestial doing this

			fmt.Println()
			fmt.Print("GET gravity: ")
			fmt.Println(msg)
			fmt.Println()
		}(msg)
	})
}

type SecretsManager struct {
	keyPrefix string
	db        *DB
	flusher   flusher.Flusher
}

func NewSyncManager(topic string, db *DB, f flusher.Flusher) *SyncManager {
	return &SyncManager{
		topic:   topic,
		db:      db,
		flusher: f,
	}
}

func (sm *SecretsManager) Start(ctx context.Context) {
	go func() {
		rch := sm.db.Client.Watch(ctx, sm.keyPrefix, clientv3.WithPrefix())
		for {
			select {
			case result := <-rch:
				for _, ev := range result.Events {
					data, err := convert(ev.Kv.Value)
					span, _ := sg.FromCustomSource(
						data.SpanContext,
						data.SpanContext.Baggage,
						"secrets.manager",
					)
					fmt.Println(span)
					defer span.Finish()

					if err != nil {
						span.AddLog(&sg.KV{"convert error", err.Error()})
						continue
					}
					sm.flusher.Flush(sg.NewTracedGRPCContext(nil, span), data)
				}
			case <-ctx.Done():
				fmt.Println(ctx.Err())
				return
			}
		}
	}()
}

func NewSecretsManager(prefix string, db *DB, f flusher.Flusher) *SecretsManager {
	return &SecretsManager{
		keyPrefix: prefix,
		db:        db,
		flusher:   f,
	}
}

type ConfigsManager struct {
	keyPrefix string
	db        *DB
	flusher   flusher.Flusher
}

func (cm *ConfigsManager) Start(ctx context.Context) {
	go func() {
		rch := cm.db.Client.Watch(ctx, cm.keyPrefix, clientv3.WithPrefix())
		for {
			select {
			case result := <-rch:
				for _, ev := range result.Events {
					data, err := convert(ev.Kv.Value)
					span, _ := sg.FromCustomSource(
						data.SpanContext,
						data.SpanContext.Baggage,
						"configs.manager",
					)
					fmt.Println(span)
					defer span.Finish()

					if err != nil {
						span.AddLog(&sg.KV{"convert error", err.Error()})
						continue
					}
					cm.flusher.Flush(sg.NewTracedContext(nil, span), data)
				}
			case <-ctx.Done():
				fmt.Println(ctx.Err())
				return
			}
		}
	}()
}

func NewConfigsManager(prefix string, db *DB, f flusher.Flusher) *ConfigsManager {
	return &ConfigsManager{
		keyPrefix: prefix,
		db:        db,
		flusher:   f,
	}
}

type ActionsManager struct {
	keyPrefix string
	db        *DB
	flusher   flusher.Flusher
}

func (am *ActionsManager) Start(ctx context.Context) {
	go func() {
		rch := am.db.Client.Watch(ctx, am.keyPrefix, clientv3.WithPrefix())
		for {
			select {
			case result := <-rch:
				for _, ev := range result.Events {
					data, err := convert(ev.Kv.Value)
					span, _ := sg.FromCustomSource(
						data.SpanContext,
						data.SpanContext.Baggage,
						"actions.manager",
					)
					fmt.Println(span)
					defer span.Finish()

					if err != nil {
						span.AddLog(&sg.KV{"convert error", err.Error()})
						continue
					}
					am.flusher.Flush(sg.NewTracedGRPCContext(ctx, span), data)
				}
			case <-ctx.Done():
				fmt.Println(ctx.Err())
				return
			}
		}
	}()
}

func NewActionsManager(prefix string, db *DB, f flusher.Flusher) *ActionsManager {
	return &ActionsManager{
		keyPrefix: prefix,
		db:        db,
		flusher:   f,
	}
}

func convert(buf []byte) (*gPb.FlushTask, error) {
	data := &gPb.FlushTask{}
	err := proto.Unmarshal(buf, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}
