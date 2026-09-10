package registry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestDiscoverConcurrentCacheUpdatesComplete(t *testing.T) {
	r := &Registry{services: map[string]map[string]Instance{"svc": {"a": {Addr: "a"}}}}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for n := 0; n < 8; n++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for i := 0; i < 2000; i++ {
				_, _ = r.Discover("svc")
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 2000; i++ {
			r.mu.Lock()
			r.services["svc"]["a"] = Instance{Addr: "a"}
			r.mu.Unlock()
		}
	}()
	close(start)
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cached discovery and cache writer deadlocked")
	}
}

type auditClosedWatcher struct{ clientv3.Watcher }

type auditBlockingKV struct {
	clientv3.KV
	entered chan struct{}
	release chan struct{}
}

func (kv *auditBlockingKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	close(kv.entered)
	select {
	case <-kv.release:
		return nil, context.Canceled
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestUncachedDiscoveryDoesNotBlockCachedServices(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kv := &auditBlockingKV{entered: make(chan struct{}), release: make(chan struct{})}
	defer close(kv.release)
	r := &Registry{client: &clientv3.Client{KV: kv}, ctx: ctx, services: map[string]map[string]Instance{"cached": {"a": {Addr: "a"}}}}
	go func() { _, _ = r.Discover("uncached") }()
	auditAwait(t, kv.entered, "uncached Get")
	done := make(chan struct{})
	go func() { _, _ = r.Discover("cached"); close(done) }()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("slow etcd Get for one service blocked discovery of an already cached service")
	}
}

func (auditClosedWatcher) Watch(context.Context, string, ...clientv3.OpOption) clientv3.WatchChan {
	ch := make(chan clientv3.WatchResponse)
	close(ch)
	return ch
}

func TestWatchStopsAfterContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Registry{client: &clientv3.Client{Watcher: auditClosedWatcher{}}, ctx: ctx}
	done := make(chan struct{})
	go func() { r.watch("svc"); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("watch goroutine did not exit after registry context cancellation")
	}
}

func TestDiscoveryInstanceOrderIsStable(t *testing.T) {
	r := &Registry{services: map[string]map[string]Instance{"svc": {}}}
	for _, addr := range []string{"h", "a", "g", "b", "f", "c", "e", "d"} {
		r.services["svc"][addr] = Instance{Addr: addr}
	}
	for i := 0; i < 100; i++ {
		got := r.copyInstances("svc")
		if !sort.SliceIsSorted(got, func(i, j int) bool { return got[i].Addr < got[j].Addr }) {
			t.Fatalf("discovery order changes positional load-balancer weights: %v", got)
		}
	}
}

// Delaying only the Watch call exposes real etcd events between the snapshot
// and subscription. Created notifications ensure assertions run after etcd
// accepted the watch, rather than depending on network timing.
type auditGatedWatcher struct {
	clientv3.Watcher
	entered    chan struct{}
	release    chan struct{}
	created    chan struct{}
	once       sync.Once
	createOnce sync.Once
}

func (w *auditGatedWatcher) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	w.once.Do(func() {
		close(w.entered)
		select {
		case <-w.release:
		case <-ctx.Done():
		}
	})
	inner := w.Watcher.Watch(ctx, key, append(opts, clientv3.WithCreatedNotify())...)
	out := make(chan clientv3.WatchResponse)
	go func() {
		defer close(out)
		for resp := range inner {
			if resp.Created {
				w.createOnce.Do(func() { close(w.created) })
			}
			select {
			case out <- resp:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func auditEtcdRegistry(t *testing.T) (*Registry, *clientv3.Client) {
	t.Helper()
	endpoint := os.Getenv("KANARPC_TEST_ETCD")
	if endpoint == "" {
		t.Skip("set KANARPC_TEST_ETCD to an isolated etcd endpoint")
	}
	r, err := NewRegistry([]string{endpoint})
	if err != nil {
		t.Fatal(err)
	}
	r.prefix = fmt.Sprintf("/kanarpc/audit/%s/%d/", t.Name(), time.Now().UnixNano())
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = r.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = cli.Delete(ctx, r.prefix, clientv3.WithPrefix())
		_ = cli.Close()
	})
	return r, cli
}

func auditAwait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestDiscoveryCoversSnapshotWatchGap(t *testing.T) {
	for _, compact := range []bool{false, true} {
		t.Run(fmt.Sprintf("compact=%t", compact), func(t *testing.T) {
			r, cli := auditEtcdRegistry(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			oldKey := r.prefix + "svc/old"
			if _, err := cli.Put(ctx, oldKey, "old"); err != nil {
				t.Fatal(err)
			}
			w := &auditGatedWatcher{Watcher: r.client.Watcher, entered: make(chan struct{}), release: make(chan struct{}), created: make(chan struct{})}
			r.client.Watcher = w
			if _, err := r.Discover("svc"); err != nil {
				t.Fatal(err)
			}
			auditAwait(t, w.entered, "watch entry")
			if _, err := cli.Delete(ctx, oldKey); err != nil {
				t.Fatal(err)
			}
			put, err := cli.Put(ctx, r.prefix+"svc/new", "new")
			if err != nil {
				t.Fatal(err)
			}
			if compact {
				if _, err := cli.Compact(ctx, put.Header.Revision); err != nil {
					t.Fatal(err)
				}
			}
			close(w.release)
			auditAwait(t, w.created, "watch creation")
			deadline := time.Now().Add(2 * time.Second)
			for {
				got := r.copyInstances("svc")
				if reflect.DeepEqual(got, []Instance{{Addr: "new"}}) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("cache missed delete/put between Get and Watch: got %v, want [new]", got)
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}
}

func TestRegistryCloseWithdrawsRegistrations(t *testing.T) {
	r, cli := auditEtcdRegistry(t)
	if err := r.Register("svc", Instance{Addr: "127.0.0.1:12345"}, 10); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := cli.Get(ctx, r.prefix+"svc/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 0 {
		t.Fatalf("registry Close left %d live registration(s) until lease expiry", len(resp.Kvs))
	}
}

type auditFailingKeepAlive struct{ clientv3.Lease }

func (auditFailingKeepAlive) KeepAlive(context.Context, clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	return nil, errors.New("injected keepalive failure")
}

func TestFailedRegistrationCleansUpLease(t *testing.T) {
	r, cli := auditEtcdRegistry(t)
	r.client.Lease = auditFailingKeepAlive{r.client.Lease}
	if err := r.Register("svc", Instance{Addr: "a"}, 10); err == nil {
		t.Fatal("expected injected failure")
	}
	resp, err := cli.Get(context.Background(), r.prefix+"svc/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 0 {
		t.Fatalf("failed Register left %d discoverable registration(s)", len(resp.Kvs))
	}
}

type auditGatedPut struct {
	clientv3.KV
	entered chan struct{}
	release chan struct{}
}

func (kv *auditGatedPut) Put(ctx context.Context, key, value string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	close(kv.entered)
	<-kv.release
	return kv.KV.Put(ctx, key, value, opts...)
}

func TestRegistryCloseWaitsForConcurrentRegistration(t *testing.T) {
	r, _ := auditEtcdRegistry(t)
	kv := &auditGatedPut{KV: r.client.KV, entered: make(chan struct{}), release: make(chan struct{})}
	r.client.KV = kv
	registrationDone := make(chan struct{})
	go func() { _ = r.Register("svc", Instance{Addr: "a"}, 10); close(registrationDone) }()
	auditAwait(t, kv.entered, "registration Put")
	closed := make(chan struct{})
	go func() { _ = r.Close(); close(closed) }()
	select {
	case <-closed:
		close(kv.release)
		auditAwait(t, registrationDone, "registration cleanup")
		t.Fatal("Close returned while a registration was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(kv.release)
	auditAwait(t, closed, "registry Close")
	auditAwait(t, registrationDone, "registration completion")
}

func TestRegistryClosePreservesReplacementRegistration(t *testing.T) {
	r, cli := auditEtcdRegistry(t)
	if err := r.Register("svc", Instance{Addr: "a"}, 10); err != nil {
		t.Fatal(err)
	}
	replacement, err := cli.Grant(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = cli.Revoke(context.Background(), replacement.ID) })
	key := r.prefix + "svc/a"
	if _, err := cli.Put(context.Background(), key, "a", clientv3.WithLease(replacement.ID)); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	resp, err := cli.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 1 || resp.Kvs[0].Lease != int64(replacement.ID) {
		t.Fatalf("Close removed registration owned by replacement lease: %v", resp.Kvs)
	}
}

func TestDiscoverContextCancellationDuringInitialization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kv := &auditBlockingKV{entered: make(chan struct{}), release: make(chan struct{})}
	defer close(kv.release)
	r := &Registry{client: &clientv3.Client{KV: kv}, ctx: ctx, services: make(map[string]map[string]Instance)}
	leaderCtx, stopLeader := context.WithCancel(ctx)
	defer stopLeader()
	leader := make(chan error, 1)
	go func() { _, err := r.DiscoverContext(leaderCtx, "svc"); leader <- err }()
	auditAwait(t, kv.entered, "initial discovery")
	waitCtx, stopWait := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stopWait()
	if _, err := r.DiscoverContext(waitCtx, "svc"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("initialization waiter: got %v", err)
	}
	stopLeader()
	select {
	case err := <-leader:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("initialization leader: got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("caller cancellation did not stop etcd read")
	}
}

type auditRestartWatcher struct {
	clientv3.Watcher
	calls      atomic.Int32
	created    chan struct{}
	disconnect chan struct{}
	reentered  chan struct{}
	release    chan struct{}
}

func (w *auditRestartWatcher) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	call := w.calls.Add(1)
	if call == 2 {
		close(w.reentered)
		select {
		case <-w.release:
		case <-ctx.Done():
		}
	}
	watchCtx, cancel := context.WithCancel(ctx)
	inner := w.Watcher.Watch(watchCtx, key, append(opts, clientv3.WithCreatedNotify())...)
	out := make(chan clientv3.WatchResponse)
	go func() {
		defer close(out)
		defer cancel()
		var disconnect <-chan struct{}
		if call == 1 {
			disconnect = w.disconnect
		}
		for {
			select {
			case <-disconnect:
				return
			case <-ctx.Done():
				return
			case resp, ok := <-inner:
				if !ok {
					return
				}
				if resp.Created && call == 1 {
					close(w.created)
				}
				select {
				case out <- resp:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

func TestDiscoveryResumesAfterWatchChannelCloses(t *testing.T) {
	r, cli := auditEtcdRegistry(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.Put(ctx, r.prefix+"svc/old", "old"); err != nil {
		t.Fatal(err)
	}
	w := &auditRestartWatcher{Watcher: r.client.Watcher, created: make(chan struct{}), disconnect: make(chan struct{}), reentered: make(chan struct{}), release: make(chan struct{})}
	r.client.Watcher = w
	if _, err := r.Discover("svc"); err != nil {
		t.Fatal(err)
	}
	auditAwait(t, w.created, "first watch")
	close(w.disconnect)
	auditAwait(t, w.reentered, "replacement watch")
	if _, err := cli.Delete(ctx, r.prefix+"svc/old"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Put(ctx, r.prefix+"svc/new", "new"); err != nil {
		t.Fatal(err)
	}
	close(w.release)
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := r.copyInstances("svc")
		if reflect.DeepEqual(got, []Instance{{Addr: "new"}}) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replacement watch lost events: got %v", got)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestConcurrentRegistryCloseIsIdempotentAndRejectsNewWork(t *testing.T) {
	r, _ := auditEtcdRegistry(t)
	if err := r.Register("svc", Instance{Addr: "a"}, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Discover("svc"); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := r.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	workers.Wait()
	if _, err := r.Discover("svc"); err == nil {
		t.Fatal("cached discovery succeeded after Close")
	}
	if err := r.Register("svc", Instance{Addr: "b"}, 10); err == nil {
		t.Fatal("registration succeeded after Close")
	}
}
