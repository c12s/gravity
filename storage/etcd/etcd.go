package etcd

import (
	"context"
	bPb "github.com/c12s/scheme/blackhole"
	gPb "github.com/c12s/scheme/gravity"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	"strconv"
	"strings"
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

func (db *DB) persist(ctx context.Context, strategy *bPb.Strategy, nodes [][]string, name string, payload []*bPb.Payload) error {
	kind := AT_ONCE
	switch strategy.Type {
	case bPb.StrategyKind_AT_ONCE:
		kind = AT_ONCE
	case bPb.StrategyKind_ROLLING_UPDATE:
		kind = ROLLING_UPDATE
	case bPb.StrategyKind_CANARY:
		kind = CANARY
	}

	part := &gPb.FlushTaskPart{Nodes: []string{}}
	for _, val := range nodes {
		copy(part.Nodes, val)
	}

	parts := []*gPb.FlushTaskPart{}
	parts = append(parts, part)

	ft := &gPb.FlushTask{
		Parts:    parts,
		Payload:  payload,
		Strategy: strategy,
	}

	cData, err := proto.Marshal(ft)
	if err != nil {
		return err
	}

	_, err = db.Kv.Put(ctx, FlushKey(name, kind), string(cData))
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) Chop(ctx context.Context, strategy *bPb.Strategy, nodes []string, name string, payload []*bPb.Payload) error {
	keys := [][]string{}
	if strategy.Kind == "all" {
		key := FlushKey(name, strategy.Kind)
		keys = append(keys, []string{key})
	} else if strings.Contains(strategy.Kind, "%") {
		splitp := []float32{}
		if strings.Contains(strategy.Kind, ",") {
			for _, pval := range strings.Split(strategy.Kind, ",") {
				i := strings.Index(pval, "%")
				snum := pval[:i]
				number, cerr := strconv.ParseFloat(snum, 32)
				if cerr != nil {
					return cerr
				}
				splitp = append(splitp, float32(number))
			}
		} else { // if user add only one value that should be 25% or 0.25
			for _, pval := range strings.Split(strategy.Kind, ",") {
				i := strings.Index(pval, "%")
				snum := pval[:i]
				number, cerr := strconv.ParseFloat(snum, 32)
				if cerr != nil {
					return cerr
				}
				splitp = append(splitp, float32(number))
			}
		}
		perr, val := PercentSplit(nodes, splitp)
		if perr != nil {
			return perr
		}
		copy(keys, val)
	} else {
		number, err := strconv.Atoi(strategy.Kind)
		if err != nil {
			return err
		}
		for _, val := range Split(number, nodes) {
			keys = append(keys, val)
		}
	}
	return db.persist(ctx, strategy, keys, name, payload)
}

func (db *DB) Filter(ctx context.Context, req *gPb.PutReq) (error, []string) {
	task := req.Task.Mutate.Task
	searchLabelsKey, kerr := SearchKey(task.RegionId, task.ClusterId)
	if kerr != nil {
		return kerr, nil
	}

	gresp, err := db.Kv.Get(ctx, searchLabelsKey, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return err, nil
	}

	rez := []string{}
	for _, item := range gresp.Kvs {
		ls := SplitLabels(string(item.Value))
		els := Labels(task.Selector.Labels)

		switch task.Selector.Kind {
		case bPb.CompareKind_ALL:
			if len(ls) == len(els) && Compare(ls, els, true) {
				rez = append(rez, TransformKey(string(item.Key)))
			}
		case bPb.CompareKind_ANY:
			if Compare(ls, els, false) {
				rez = append(rez, TransformKey(string(item.Key)))
			}
		}
	}
	return nil, rez
}
