package etcd

import "context"

type ControllManager struct {
}

func NewControllManager() *ControllManager {
	return &ControllManager{}
}

func (cm *ControllManager) Start() {}

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
