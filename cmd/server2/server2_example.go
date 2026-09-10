package main

import (
	"github.com/KanaDoodle/KanaRPC-Go/internal/codec"
	"github.com/KanaDoodle/KanaRPC-Go/internal/registry"
	"github.com/KanaDoodle/KanaRPC-Go/internal/server"
	"github.com/KanaDoodle/KanaRPC-Go/pkg/api"
	"log"
	"os"
	"os/signal"
)

func main() {
	reg, err := registry.NewRegistry([]string{"localhost:2379"})
	if err != nil {
		log.Fatal(err)
	}
	defer reg.Close()

	srv, err := server.NewServer(
		":9091",
		server.WithServerCodec(codec.JSON),
		server.WithServerRateLimit(1_000_000),
	)
	if err != nil {
		log.Println("server.NewServer error ", err.Error())
		return
	}
	// 注册 Arith 服务
	srv.Register("Arith", &api.Arith{})

	// 注册服务到 etcd
	err = reg.Register("Arith", registry.Instance{
		Addr: "localhost:9091",
	}, 10)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("server started at :9091")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	go func() {
		<-sigCh
		log.Println("graceful shutdown...")
		srv.Shutdown()
	}()

	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
}
