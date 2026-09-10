package rpc_test

import (
	"context"
	"fmt"
	"github.com/KanaDoodle/KanaRPC-Go/rpc"
	"os"
	"testing"
	"time"
)

type Request struct{ Value int }
type Handler struct{}

func (*Handler) Echo(req *Request, resp *Request) error { resp.Value = req.Value; return nil }
func TestPublicFacade(t *testing.T) {
	endpoint := os.Getenv("KANARPC_TEST_ETCD")
	if endpoint == "" {
		t.Skip("requires etcd")
	}
	r, err := rpc.NewRegistry([]string{endpoint})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Ready() {
		t.Fatal("unregistered registry ready")
	}
	s, err := rpc.NewServer(rpc.ServerConfig{Address: "127.0.0.1:0", MaxConcurrentRequests: 2})
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("facade-%d", time.Now().UnixNano())
	s.Register(name, &Handler{})
	done := make(chan error, 1)
	go func() { done <- s.Start() }()
	defer func() { s.Shutdown(); <-done }()
	until := time.Now().Add(time.Second)
	for s.Addr() == nil && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if s.Addr() == nil {
		t.Fatal("listener missing")
	}
	if err = r.Register(name, s.Addr().String(), 5); err != nil {
		t.Fatal(err)
	}
	for _, selection := range []rpc.Selection{rpc.RoundRobin, rpc.Random} {
		c, e := rpc.NewClient(r, rpc.ClientConfig{Timeout: time.Second, Selection: selection})
		if e != nil {
			t.Fatal(e)
		}
		defer c.Close()
		var out Request
		if err = c.Invoke(context.Background(), name, "Echo", &Request{42}, &out); err != nil || out.Value != 42 {
			t.Fatal(err, out)
		}
	}
	if !r.Ready() {
		t.Fatal("registered service not ready")
	}
}
