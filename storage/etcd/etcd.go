package etcd

import (
	"context"
	"fmt"
	"github.com/c12s/gravity/storage"
	bPb "github.com/c12s/scheme/blackhole"
	gPb "github.com/c12s/scheme/gravity"
	sPb "github.com/c12s/scheme/stellar"
	sg "github.com/c12s/stellar-go"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	"strconv"
	"strings"
	"time"
)

type DB struct {
	Kv          clientv3.KV
	Client      *clientv3.Client
	Controllers []storage.ControllManager
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

func (db *DB) StartControllers(ctx context.Context) {
	for _, manager := range db.Controllers {
		manager.Start(ctx)
	}
}

func (db *DB) RegisterControllers(controllers []storage.ControllManager) {
	db.Controllers = append(db.Controllers, controllers...)
}

func parse(tags string) map[string]string {
	rez := map[string]string{}
	if len(tags) > 0 {
		for _, item := range strings.Split(tags, ";") {

			fmt.Println("SPLIT ", item)
			pair := strings.Split(item, ":")
			fmt.Println("SPLIT ", pair)
			rez[pair[0]] = pair[1]
		}
	}

	return rez
}

func (db *DB) persist(ctx context.Context, strategy *bPb.Strategy, nodes [][]string, name, kind, taskKey string, payload []*bPb.Payload) error {
	span, _ := sg.FromGRPCContext(ctx, "persist")
	defer span.Finish()
	fmt.Println(span)
	fmt.Println("SERIALIZE ", span.Serialize())

	parts := []*gPb.FlushTaskPart{}
	for _, val := range nodes {
		part := &gPb.FlushTaskPart{Nodes: make([]string, len(val))}
		copy(part.Nodes, val)
		parts = append(parts, part)
	}

	ssp := span.Serialize()
	fmt.Println("LUDILO:", ssp.Get("tags"))
	ft := &gPb.FlushTask{
		Parts:    parts,
		Payload:  payload,
		Strategy: strategy,
		TaskKey:  taskKey,
		SpanContext: &sPb.SpanContext{
			TraceId:       ssp.Get("trace_id")[0],
			SpanId:        ssp.Get("span_id")[0],
			ParrentSpanId: ssp.Get("parrent_span_id")[0],
			Baggage:       parse(ssp.Get("tags")[0]),
		},
	}

	cData, err := proto.Marshal(ft)
	if err != nil {
		span.AddLog(&sg.KV{"marshall error", err.Error()})
		return err
	}

	child := span.Child("etcd put")
	_, err = db.Kv.Put(ctx, FlushKey(name, kind), string(cData))
	if err != nil {
		child.AddLog(&sg.KV{"etcd put error", err.Error()})
		return err
	}
	child.Finish()

	return nil
}

func (db *DB) Chop(ctx context.Context, strategy *bPb.Strategy, nodes []string, name, kind, taskKey string, payload []*bPb.Payload) error {
	span, _ := sg.FromGRPCContext(ctx, "chop")
	defer span.Finish()
	fmt.Println(span)
	fmt.Println("SERIALIZE ", span.Serialize())

	keys := [][]string{}
	if strategy.Kind == "all" {
		keys = append(keys, nodes)
	} else if strings.Contains(strategy.Kind, "%") {
		splitp := []float32{}
		if strings.Contains(strategy.Kind, ",") {
			for _, pval := range strings.Split(strategy.Kind, ",") {
				i := strings.Index(pval, "%")
				snum := pval[:i]
				number, cerr := strconv.ParseFloat(snum, 32)
				if cerr != nil {
					span.AddLog(&sg.KV{"parse error", cerr.Error()})
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
					span.AddLog(&sg.KV{"parse error", cerr.Error()})
					return cerr
				}
				splitp = append(splitp, float32(number))
			}
		}
		perr, val := PercentSplit(nodes, splitp)
		if perr != nil {
			span.AddLog(&sg.KV{"percent split error", perr.Error()})
			return perr
		}
		copy(keys, val)
	} else {
		number, err := strconv.Atoi(strategy.Kind)
		if err != nil {
			span.AddLog(&sg.KV{"strategy error", err.Error()})
			return err
		}
		for _, val := range Split(number, nodes) {
			keys = append(keys, val)
		}
	}
	return db.persist(sg.NewTracedGRPCContext(ctx, span), strategy, keys, name, kind, taskKey, payload)
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
