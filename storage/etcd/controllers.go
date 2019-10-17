package etcd

import (
	"context"
	"fmt"
	flusher "github.com/c12s/gravity/flush"
	fPb "github.com/c12s/scheme/flusher"
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
	rch := sm.db.Client.Watch(ctx, sm.keyPrefix, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			data, err := convert(ev.Kv.Value)
			if err != nil {
				continue
				//TODO: Send error data to log!
			}
			sm.flusher.Flush(ctx, "", data)
		}
	}
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
	rch := cm.db.Client.Watch(ctx, cm.keyPrefix, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			data, err := convert(ev.Kv.Value)
			if err != nil {
				continue
				//TODO: Send error data to log!
			}
			cm.flusher.Flush(ctx, "", data)
		}
	}
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
	rch := am.db.Client.Watch(ctx, am.keyPrefix, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			data, err := convert(ev.Kv.Value)
			if err != nil {
				continue
				//TODO: Send error data to log!
			}
			am.flusher.Flush(ctx, "", data)
		}
	}
}

func NewActionsManager(prefix string, db *DB, f flusher.Flusher) *ActionsManager {
	return &ActionsManager{
		keyPrefix: prefix,
		db:        db,
		flusher:   f,
	}
}

func convert(buf []byte) (*fPb.FlushPush, error) {
	data := &gPb.FlushTask{}
	err := proto.Unmarshal(buf, data)
	if err != nil {
		return nil, err
	}

	return &fPb.FlushPush{
		Payload: data.Payload,
	}, nil
}
