package etcd

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"time"
)

type DB struct {
	Kv     clientv3.KV
	Client *clientv3.Client
}

func New(endpoints []string, timeout time.Duration) (*DB, error) {
	cli, err := clientv3.New(clientv3.Config{
		DialTimeout: timeout,
		Endpoints:   endpoints,
	})

	if err != nil {
		return nil, err
	}

	return &DB{
		Kv:     clientv3.NewKV(cli),
		Client: cli,
	}, nil
}

func (db *DB) Close() { db.Client.Close() }

func (db *DB) Update(ctx context.Context, key, data string) error {
	return nil
}

func (db *DB) Chop(ctx context.Context, labels map[string]string) error {
	return nil
}
