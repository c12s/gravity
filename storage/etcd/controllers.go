package etcd

import (
	"context"
	"fmt"
	flusher "github.com/c12s/gravity/flush"
	gPb "github.com/c12s/scheme/gravity"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
)

type SecretsManager struct {
	keyPrefix string
	db        *DB
	flusher   flusher.Flusher
}

func (sm *SecretsManager) Start(ctx context.Context) {
	go func() {
		rch := sm.db.Client.Watch(ctx, sm.keyPrefix, clientv3.WithPrefix())
		for {
			select {
			case result := <-rch:
				for _, ev := range result.Events {
					data, err := convert(ev.Kv.Value)
					if err != nil {
						continue
						//TODO: Send error data to log!
					}
					sm.flusher.Flush(ctx, data)
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
					if err != nil {
						continue
						//TODO: Send error data to log!
					}
					cm.flusher.Flush(ctx, data)
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
					if err != nil {
						continue
						//TODO: Send error data to log!
					}
					am.flusher.Flush(ctx, data)
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
