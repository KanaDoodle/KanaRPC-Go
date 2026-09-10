package client_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC-Go/internal/client"
	"github.com/KanaDoodle/KanaRPC-Go/internal/codec"
	"github.com/KanaDoodle/KanaRPC-Go/internal/registry"
	"github.com/KanaDoodle/KanaRPC-Go/internal/server"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type protoAuditService struct{}

func (*protoAuditService) Echo(req *wrapperspb.Int64Value, reply *wrapperspb.Int64Value) error {
	reply.Value = req.Value + 1
	return nil
}

func TestAuditConfiguredCodecEndToEnd(t *testing.T) {
	endpoint := os.Getenv("KANARPC_TEST_ETCD")
	if endpoint == "" {
		t.Skip("set KANARPC_TEST_ETCD to an isolated etcd endpoint")
	}
	for _, codecType := range []codec.Type{codec.JSON, codec.PROTO} {
		t.Run(fmt.Sprint(codecType), func(t *testing.T) {
			reg, err := registry.NewRegistry([]string{endpoint})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reg.Close() })
			srv, err := server.NewServer("127.0.0.1:0", server.WithServerCodec(codecType))
			if err != nil {
				t.Fatal(err)
			}
			service := fmt.Sprintf("audit-codec-%d-%d", codecType, time.Now().UnixNano())
			srv.Register(service, &protoAuditService{})
			started := make(chan error, 1)
			go func() { started <- srv.Start() }()
			t.Cleanup(func() { srv.Close(); <-started })
			deadline := time.Now().Add(2 * time.Second)
			for srv.Addr() == nil && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if srv.Addr() == nil {
				t.Fatal("server did not start")
			}
			if err := reg.Register(service, registry.Instance{Addr: srv.Addr().String()}, 5); err != nil {
				t.Fatal(err)
			}
			c, err := client.NewClient(reg, client.WithClientCodec(codecType))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(c.Close)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			reply := &wrapperspb.Int64Value{}
			if err := c.Invoke(ctx, service, "Echo", wrapperspb.Int64(41), reply); err != nil {
				t.Fatalf("configured codec %d RPC failed: %v", codecType, err)
			}
			if reply.Value != 42 {
				t.Fatalf("got %d, want 42", reply.Value)
			}
		})
	}
}
