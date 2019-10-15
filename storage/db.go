package storage

import (
	"context"
	bPb "github.com/c12s/scheme/blackhole"
	gPb "github.com/c12s/scheme/gravity"
)

type DB interface {
	Update(ctx context.Context, key, data string) error
	Chop(ctx context.Context, strategy *bPb.Strategy, nodes []string, name string, payload []*bPb.Payload) error
	Filter(ctx context.Context, req *gPb.PutReq) (error, []string)
	Close()
}
