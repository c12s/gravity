package etcd

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
)

type SecretsManager struct {
	keyPrefix string
	db        *DB
}

func (sm *SecretsManager) Start(ctx context.Context) {
	rch := sm.db.Client.Watch(ctx, sm.keyPrefix, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
		}
	}
}

func NewSecretsManager(prefix string, db *DB) *SecretsManager {
	return &SecretsManager{
		keyPrefix: prefix,
		db:        db,
	}
}

type ConfigsManager struct {
	keyPrefix string
	db        *DB
}

func (cm *ConfigsManager) Start(ctx context.Context) {
	rch := cm.db.Client.Watch(ctx, cm.keyPrefix, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
		}
	}
}

func NewConfigsManager(prefix string, db *DB) *ConfigsManager {
	return &ConfigsManager{
		keyPrefix: prefix,
		db:        db,
	}
}

type ActionsManager struct {
	keyPrefix string
	db        *DB
}

func (am *ActionsManager) Start(ctx context.Context) {
	rch := am.db.Client.Watch(ctx, am.keyPrefix, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
		}
	}
}

func NewActionsManager(prefix string, db *DB) *ActionsManager {
	return &ActionsManager{
		keyPrefix: prefix,
		db:        db,
	}
}
