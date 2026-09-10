package client

import (
	"context"
	"fmt"
	"github.com/KanaDoodle/KanaRPC-Go/internal/breaker"
	"github.com/KanaDoodle/KanaRPC-Go/internal/registry"
	"github.com/KanaDoodle/KanaRPC-Go/internal/server"
	"os"
	"testing"
	"time"
)

type RepairRequest struct{ N int }
type repairService struct{ delay time.Duration }

func (s *repairService) Echo(req *RepairRequest, resp *RepairRequest) error {
	time.Sleep(s.delay)
	resp.N = req.N
	return nil
}
func TestRepairBreakerSelection(t *testing.T) {
	ep := os.Getenv("KANARPC_TEST_ETCD")
	if ep == "" {
		t.Skip("requires etcd")
	}
	r, err := registry.NewRegistry([]string{ep})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	name := fmt.Sprintf("repair-%d", time.Now().UnixNano())
	addrs := []string{}
	for i := 0; i < 2; i++ {
		s, e := server.NewServer("127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		s.Register(name, &repairService{})
		done := make(chan error, 1)
		go func() { done <- s.Start() }()
		t.Cleanup(func() { s.Close(); <-done })
		until := time.Now().Add(time.Second)
		for s.Addr() == nil && time.Now().Before(until) {
			time.Sleep(time.Millisecond)
		}
		if s.Addr() == nil {
			t.Fatal("no listener")
		}
		addr := s.Addr().String()
		addrs = append(addrs, addr)
		if e = r.Register(name, registry.Instance{Addr: addr}, 5); e != nil {
			t.Fatal(e)
		}
	}
	c, e := NewClient(r)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	bad := c.getBreaker(name, addrs[0])
	for i := 0; i < 10; i++ {
		bad.RecordFailure()
	}
	if bad.State() != breaker.Open {
		t.Fatal("not open")
	}
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		var out RepairRequest
		e = c.Invoke(ctx, name, "Echo", &RepairRequest{N: 42}, &out)
		cancel()
		if e != nil || out.N != 42 {
			t.Errorf("F08: healthy instance not selected: %v", e)
		}
	}
	for i := 0; i < 10; i++ {
		c.getBreaker(name, addrs[1]).RecordFailure()
	}
	start := time.Now()
	var out RepairRequest
	if e = c.Invoke(context.Background(), name, "Echo", &RepairRequest{}, &out); e == nil {
		t.Error("all open accepted")
	}
	if time.Since(start) > time.Second {
		t.Error("unbounded selection")
	}
}

func TestRepairLocalCancellationAndBackendTimeout(t *testing.T) {
	ep := os.Getenv("KANARPC_TEST_ETCD")
	if ep == "" {
		t.Skip("requires etcd")
	}
	reg, err := registry.NewRegistry([]string{ep})
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	srv, err := server.NewServer("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("repair-cancel-%d", time.Now().UnixNano())
	srv.Register(name, &repairService{delay: 50 * time.Millisecond})
	done := make(chan error, 1)
	go func() { done <- srv.Start() }()
	defer func() { srv.Close(); <-done }()
	until := time.Now().Add(time.Second)
	for srv.Addr() == nil && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if srv.Addr() == nil {
		t.Fatal("no listener")
	}
	addr := srv.Addr().String()
	if err = reg.Register(name, registry.Instance{Addr: addr}, 5); err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(reg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := 0; i < 12; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		f, e := c.InvokeAsync(ctx, name, "Echo", &RepairRequest{})
		if e != nil {
			cancel()
			t.Fatal(e)
		}
		cancel()
		_, _ = f.Wait()
	}
	time.Sleep(10 * time.Millisecond)
	if c.getBreaker(name, addr).State() != breaker.Closed {
		t.Fatal("F08: local context cancellation attributed to backend")
	}
	for i := 0; i < 12; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		var out RepairRequest
		_ = c.Invoke(ctx, name, "Echo", &RepairRequest{}, &out)
		cancel()
	}
	time.Sleep(10 * time.Millisecond)
	if c.getBreaker(name, addr).State() != breaker.Open {
		t.Fatal("backend deadline stopped affecting breaker")
	}
	// A fresh half-open candidate becomes eligible and receives exactly one probe.
	probe := breaker.NewCircuitBreaker(1, .5, time.Millisecond)
	probe.RecordFailure()
	c.breaker.Store(name+"|"+addr, probe)
	time.Sleep(3 * time.Millisecond)
	var out RepairRequest
	if e := c.Invoke(context.Background(), name, "Echo", &RepairRequest{N: 1}, &out); e != nil {
		t.Fatal(e)
	}
	if probe.State() != breaker.Closed {
		t.Fatal("half-open did not recover")
	}
}
