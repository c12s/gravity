package main

import (
	"fmt"
	"github.com/c12s/gravity/config"
	"github.com/c12s/gravity/flush/nats"
	"github.com/c12s/gravity/service"
	"github.com/c12s/gravity/storage/etcd"
	"time"
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

	service.Run(conf, db, f)
}
