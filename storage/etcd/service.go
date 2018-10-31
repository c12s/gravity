package etcd

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"strings"
	"time"
)

const key = "topology/"

type EtcdReconcile struct {
	Kv     clientv3.KV
	Client *clientv3.Client
}

func New(ctx context.Context, endpoints []string, timeout time.Duration) (*EtcdReconcile, error) {
	cli, err := clientv3.New(clientv3.Config{
		DialTimeout: timeout,
		Endpoints:   endpoints,
	})

	if err != nil {
		return nil, err
	}

	return &EtcdReconcile{
		Kv:     kv,
		Client: cli,
	}, nil
}

func (r *EtcdReconcile) Start(ctx context.Context) {
	go func() {
		watchChan := cli.Watch(ctx, key, clientv3.WithPrefix())
		for {
			select {
			case result := <-watchChan:
				for _, ev := range result.Events {
					if !strings.Contains("labels", ec.Kv.Key) {
						fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
					}
				}
			case <-ctx.Done():
				fmt.Println(ctx.Err())
				return
			}
		}
	}()

}

func (r *EtcdReconcile) Flush(topic string) {

}

func (r *EtcdReconcile) Close() { r.Client.Close() }
