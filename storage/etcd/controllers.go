package etcd

import "context"

func controllManager(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		}

	}
}

func atonce(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		}
	}
}
