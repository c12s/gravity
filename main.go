package main

import (
	"fmt"
	"github.com/c12s/gravity/config"
	"github.com/c12s/gravity/flush/nats"
	"github.com/c12s/gravity/service"
	"github.com/c12s/gravity/storage"
	"github.com/c12s/gravity/storage/etcd"
	"time"
)

const (
	secrets  = "flush/secrets/"
	configs  = "flush/configs/"
	actions  = "flush/actions/"
	topology = "flush/topology/"
	sync     = "syncTopic"
)

func main() {
	conf, err := config.ConfigFile()
	if err != nil {
		fmt.Println(err)
		return
	}

	db, err := etcd.New(conf.Endpoints, 10*time.Second)
	if err != nil {
		fmt.Println(err)
		return
	}

	f, err := nats.New(conf.Flusher)
	if err != nil {
		fmt.Println(err)
		return
	}

	db.RegisterControllers([]storage.ControllManager{
		etcd.NewSecretsManager(secrets, db, f),
		etcd.NewConfigsManager(configs, db, f),
		etcd.NewActionsManager(actions, db, f),
		etcd.NewTopologyManager(topology, db, f),
		etcd.NewSyncManager(sync, db, f),
	})

	service.Run(conf, db)
}
