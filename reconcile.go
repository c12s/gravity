package main

import "context"

type Reconciler interface {
	Start(ctx context.Context)
	Flush(topic string)
}
