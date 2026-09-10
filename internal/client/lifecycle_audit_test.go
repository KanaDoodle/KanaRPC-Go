package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC-Go/internal/breaker"
	"github.com/KanaDoodle/KanaRPC-Go/internal/registry"
)

func TestAuditClosedClientCannotCreateLivePool(t *testing.T) {
	c, err := NewClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	pool, err := c.getPool(listener.Addr().String())
	if pool != nil || !errors.Is(err, ErrClientClosed) {
		if pool != nil {
			pool.Close()
		}
		t.Fatalf("closed Client created a connection pool: pool=%v err=%v", pool != nil, err)
	}
	if _, err := c.InvokeAsync(context.Background(), "Arith", "Add", nil); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("InvokeAsync after Close = %v, want ErrClientClosed", err)
	}
}

func TestAuditHalfOpenEarlyFailuresCompleteProbe(t *testing.T) {
	endpoint := os.Getenv("KANARPC_TEST_ETCD")
	if endpoint == "" {
		t.Skip("set KANARPC_TEST_ETCD to an isolated etcd endpoint")
	}
	for _, stage := range []string{"acquire", "marshal"} {
		t.Run(stage, func(t *testing.T) {
			reg, err := registry.NewRegistry([]string{endpoint})
			if err != nil {
				t.Fatal(err)
			}
			defer reg.Close()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			addr := listener.Addr().String()
			service := fmt.Sprintf("audit-probe-%s-%d", stage, time.Now().UnixNano())
			if err := reg.Register(service, registry.Instance{Addr: addr}, 5); err != nil {
				t.Fatal(err)
			}
			var args interface{} = struct{}{}
			if stage == "acquire" {
				_ = listener.Close()
			} else {
				args = func() {}
			}
			c, err := NewClient(reg, WithClientTimeout(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			br := breaker.NewCircuitBreaker(1, 1, 0)
			br.RecordFailure()
			c.breaker.Store(service+"|"+addr, br)
			if _, err := c.InvokeAsync(context.Background(), service, "Call", args); err == nil {
				t.Fatal("expected a failure before a Future is returned")
			}
			if br.State() != breaker.Open {
				t.Fatalf("%s error left probe in state %v, want Open so a later probe can retry", stage, br.State())
			}
		})
	}
}
