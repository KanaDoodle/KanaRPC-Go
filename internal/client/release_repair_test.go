package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/KanaDoodle/KanaRPC-Go/internal/breaker"
	"github.com/KanaDoodle/KanaRPC-Go/internal/registry"
	"github.com/KanaDoodle/KanaRPC-Go/internal/server"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type reService struct {
	calls atomic.Int64
	fail  bool
}

func (s *reService) Echo(req *RepairRequest, out *RepairRequest) error {
	s.calls.Add(1)
	out.N = req.N
	if s.fail {
		return errors.New("intentional business failure after mutation")
	}
	return nil
}
func TestReleaseBreakerRecoveryUnderLoad(t *testing.T) {
	if os.Getenv("KANARPC_TEST_ETCD") == "" {
		t.Skip("requires real etcd")
	}
	reg, e := registry.NewRegistry([]string{os.Getenv("KANARPC_TEST_ETCD")})
	if e != nil {
		t.Fatal(e)
	}
	defer reg.Close()
	name := fmt.Sprintf("re-audit-%d", time.Now().UnixNano())
	services := map[string]*reService{}
	for i := 0; i < 2; i++ {
		srv, e := server.NewServer("127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		s := &reService{}
		srv.Register(name, s)
		done := make(chan error, 1)
		go func() { done <- srv.Start() }()
		t.Cleanup(func() { srv.Close(); <-done })
		end := time.Now().Add(time.Second)
		for srv.Addr() == nil && time.Now().Before(end) {
			time.Sleep(time.Millisecond)
		}
		if srv.Addr() == nil {
			t.Fatal("not listening")
		}
		addr := srv.Addr().String()
		services[addr] = s
		if e = reg.Register(name, registry.Instance{Addr: addr}, 5); e != nil {
			t.Fatal(e)
		}
	}
	xs, e := reg.Discover(name)
	if e != nil {
		t.Fatal(e)
	}
	c, e := NewClient(reg)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	badAddr := xs[1].Addr
	bad := breaker.NewCircuitBreaker(1, .5, time.Hour)
	bad.RecordFailure()
	c.breaker.Store(name+"|"+badAddr, bad)
	for i := 0; i < 300; i++ {
		var out RepairRequest
		if e = c.Invoke(context.Background(), name, "Echo", &RepairRequest{N: 7}, &out); e != nil || out.N != 7 {
			t.Fatalf("healthy selection failed %v", e)
		}
	}
	t.Logf("OPEN filtering healthy=%d open=%d", services[xs[0].Addr].calls.Load(), services[badAddr].calls.Load())
	if services[badAddr].calls.Load() != 0 {
		t.Error("OPEN selected")
	}
	probe := breaker.NewCircuitBreaker(1, .5, time.Millisecond)
	probe.RecordFailure()
	c.breaker.Store(name+"|"+badAddr, probe)
	time.Sleep(3 * time.Millisecond)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var out RepairRequest
			if e := c.Invoke(context.Background(), name, "Echo", &RepairRequest{N: 8}, &out); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if probe.State() != breaker.Closed {
		t.Error("half-open starved alongside healthy")
	}
	held := breaker.NewCircuitBreaker(1, .5, time.Millisecond)
	held.RecordFailure()
	time.Sleep(3 * time.Millisecond)
	var admitted atomic.Int32
	var finishMu sync.Mutex
	finishes := []func(error){}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if f, ok := held.Acquire(); ok {
				admitted.Add(1)
				finishMu.Lock()
				finishes = append(finishes, f)
				finishMu.Unlock()
			}
		}()
	}
	wg.Wait()
	for _, f := range finishes {
		f(context.Canceled)
	}
	t.Logf("half-open concurrent Acquire admitted=%d", admitted.Load())
	if admitted.Load() != 1 {
		t.Error("concurrent probes", admitted.Load())
	}
	if f, ok := held.Acquire(); !ok {
		t.Error("cancelled probe slot stuck")
	} else {
		f(nil)
	}
	for _, ins := range xs {
		cb := breaker.NewCircuitBreaker(1, .5, time.Hour)
		cb.RecordFailure()
		c.breaker.Store(name+"|"+ins.Addr, cb)
	}
	start := time.Now()
	var out RepairRequest
	if e = c.Invoke(context.Background(), name, "Echo", &RepairRequest{}, &out); !errors.Is(e, ErrNoUsableInstance) {
		t.Error(e)
	}
	t.Logf("all OPEN duration=%s", time.Since(start))
	if time.Since(start) > time.Second {
		t.Error("unbounded")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e = c.Invoke(ctx, name, "Echo", &RepairRequest{}, &out); !errors.Is(e, context.Canceled) {
		t.Error("cancel before selection", e)
	}
}
func TestReleaseNoBusinessReplayAndAddressState(t *testing.T) {
	if os.Getenv("KANARPC_TEST_ETCD") == "" {
		t.Skip("requires real etcd")
	}
	r, e := registry.NewRegistry([]string{os.Getenv("KANARPC_TEST_ETCD")})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	name := fmt.Sprintf("no-replay-%d", time.Now().UnixNano())
	srv, e := server.NewServer("127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	s := &reService{fail: true}
	srv.Register(name, s)
	done := make(chan error, 1)
	go func() { done <- srv.Start() }()
	defer func() { srv.Close(); <-done }()
	for srv.Addr() == nil {
		time.Sleep(time.Millisecond)
	}
	addr := srv.Addr().String()
	if e = r.Register(name, registry.Instance{Addr: addr}, 3); e != nil {
		t.Fatal(e)
	}
	c, e := NewClient(r)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	var out RepairRequest
	e = c.Invoke(context.Background(), name, "Echo", &RepairRequest{}, &out)
	t.Logf("business returned=%v invocation_count=%d", e, s.calls.Load())
	if e == nil || s.calls.Load() != 1 {
		t.Error("business replay or lost failure")
	}
	before := c.getBreaker(name, addr)
	for i := 0; i < 10; i++ {
		before.RecordFailure()
	}
	after := c.getBreaker(name, addr)
	t.Logf("address has no incarnation; same breaker object=%v state=%v", before == after, after.State())
}
func TestReleaseCrossServiceHalfOpenFairness(t *testing.T) {
	if os.Getenv("KANARPC_TEST_ETCD") == "" {
		t.Skip("requires real etcd")
	}
	r, e := registry.NewRegistry([]string{os.Getenv("KANARPC_TEST_ETCD")})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	name := fmt.Sprintf("fairness-%d", time.Now().UnixNano())
	for _, a := range []string{"a", "b"} {
		if e = r.Register(name, registry.Instance{Addr: a}, 3); e != nil {
			t.Fatal(e)
		}
	}
	if e = r.Register(name+"other", registry.Instance{Addr: "z"}, 3); e != nil {
		t.Fatal(e)
	}
	c, e := NewClient(r)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	b := breaker.NewCircuitBreaker(1, .5, time.Millisecond)
	b.RecordFailure()
	c.breaker.Store(name+"|b", b)
	time.Sleep(3 * time.Millisecond)
	probes := 0
	for i := 0; i < 100; i++ {
		addr, f, e := c.selectInstance(context.Background(), name)
		if e != nil {
			t.Fatal(e)
		}
		if addr == "b" {
			probes++
		}
		f(nil)
		_, g, e := c.selectInstance(context.Background(), name+"other")
		if e != nil {
			t.Fatal(e)
		}
		g(nil)
	}
	t.Logf("100 target calls interleaved with another service: eligible half-open probes=%d state=%v", probes, b.State())
	if probes == 0 {
		t.Error("shared round-robin counter can starve eligible half-open in multi-service client")
	}
}
