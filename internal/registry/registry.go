package registry

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Instance struct {
	Addr string
}

type Registry struct {
	client *clientv3.Client
	prefix string

	mu            sync.RWMutex
	services      map[string]map[string]Instance // service -> addr -> Instance
	initializing  map[string]chan struct{}
	revisions     map[string]int64
	closed        bool
	wg            sync.WaitGroup
	closeOnce     sync.Once
	closeErr      error
	leases        map[clientv3.LeaseID]struct{}
	registrations map[string]registrationState

	ctx    context.Context
	cancel context.CancelFunc
}

func NewRegistry(endpoints []string) (*Registry, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	// Fail fast during startup so a missing etcd does not look like a hung process.
	if err := verifyEndpoints(cli, endpoints); err != nil {
		_ = cli.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Registry{
		client:   cli,
		prefix:   "/kanarpc/services/",
		services: make(map[string]map[string]Instance),
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

// Discover initializes a service snapshot and watch on its first call.
func (r *Registry) Discover(service string) ([]Instance, error) {
	return r.DiscoverContext(context.Background(), service)
}

// DiscoverContext also bounds the initial etcd read and waits for concurrent
// initialization. Network operations never hold the shared cache lock.
func (r *Registry) DiscoverContext(ctx context.Context, service string) ([]Instance, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return nil, errors.New("registry closed")
		}
		if _, ok := r.services[service]; ok {
			instances := r.copyInstancesLocked(service)
			r.mu.Unlock()
			return instances, nil
		}
		if ready := r.initializing[service]; ready != nil {
			r.mu.Unlock()
			select {
			case <-ready:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-r.ctx.Done():
				return nil, r.ctx.Err()
			}
		}
		if r.initializing == nil {
			r.initializing = make(map[string]chan struct{})
		}
		ready := make(chan struct{})
		r.initializing[service] = ready
		r.wg.Add(1)
		r.mu.Unlock()

		instances, revision, err := r.snapshot(ctx, service)
		r.mu.Lock()
		if err == nil && !r.closed {
			if r.revisions == nil {
				r.revisions = make(map[string]int64)
			}
			r.services[service] = instances
			r.revisions[service] = revision
			r.wg.Add(1)
			go func() { defer r.wg.Done(); r.watch(service) }()
		}
		if r.closed && err == nil {
			err = errors.New("registry closed")
		}
		delete(r.initializing, service)
		close(ready)
		r.mu.Unlock()
		r.wg.Done()
		if err != nil {
			return nil, err
		}
		return r.copyInstances(service), nil
	}
}

func (r *Registry) snapshot(parent context.Context, service string) (map[string]Instance, int64, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	stop := context.AfterFunc(r.ctx, cancel)
	defer stop()
	defer cancel()
	resp, err := r.client.Get(ctx, r.prefix+service+"/", clientv3.WithPrefix())
	if err != nil {
		return nil, 0, err
	}
	instances := make(map[string]Instance, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		addr := string(kv.Value)
		instances[addr] = Instance{Addr: addr}
	}
	return instances, resp.Header.Revision + 1, nil
}

func (r *Registry) watch(service string) {
	key := fmt.Sprintf("%s%s/", r.prefix, service)
	r.mu.RLock()
	revision := r.revisions[service]
	r.mu.RUnlock()

	for {
		if r.ctx.Err() != nil {
			return
		}
		watchCtx, cancel := context.WithCancel(r.ctx)
		watchChan := r.client.Watch(watchCtx, key, clientv3.WithPrefix(), clientv3.WithRev(revision))
		resync := false

		for watchResp := range watchChan {
			if watchResp.Canceled || watchResp.Err() != nil {
				resync = watchResp.CompactRevision != 0
				break
			}
			r.mu.Lock()
			for _, event := range watchResp.Events {
				switch event.Type {

				case clientv3.EventTypePut:
					addr := string(event.Kv.Value)
					r.services[service][addr] = Instance{Addr: addr}

				case clientv3.EventTypeDelete:
					deletedKey := string(event.Kv.Key)
					addr := strings.TrimPrefix(deletedKey, r.prefix+service+"/")
					delete(r.services[service], addr)
				}
				if event.Kv.ModRevision >= revision {
					revision = event.Kv.ModRevision + 1
				}
			}
			r.mu.Unlock()
		}
		cancel()
		if r.ctx.Err() != nil {
			return
		}
		if resync {
			instances, next, err := r.snapshot(r.ctx, service)
			if err == nil {
				r.mu.Lock()
				r.services[service] = instances
				r.mu.Unlock()
				revision = next
				continue
			}
		}

		// watch 断了，稍后重连
		timer := time.NewTimer(time.Second)
		select {
		case <-r.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (r *Registry) copyInstances(service string) []Instance {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.copyInstancesLocked(service)
}

func (r *Registry) copyInstancesLocked(service string) []Instance {
	var instances []Instance
	for _, ins := range r.services[service] {
		instances = append(instances, ins)
	}
	sort.Slice(instances, func(i, j int) bool { return instances[i].Addr < instances[j].Addr })
	return instances
}

func (r *Registry) Close() error {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.cancel()
		r.mu.Unlock()
		r.wg.Wait()

		// Revoke only leases created by this registry. A replacement instance
		// may have moved the same key to its own lease while we were shutting down.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for id := range r.leases {
			_, err := r.client.Revoke(ctx, id)
			if !errors.Is(err, rpctypes.ErrLeaseNotFound) {
				r.closeErr = errors.Join(r.closeErr, err)
			}
		}
		r.closeErr = errors.Join(r.closeErr, r.client.Close())
	})
	return r.closeErr
}

func verifyEndpoints(cli *clientv3.Client, endpoints []string) error {
	for _, endpoint := range endpoints {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := cli.Status(ctx, endpoint)
		cancel()
		if err == nil {
			return nil
		}
	}

	if len(endpoints) == 0 {
		return errors.New("no etcd endpoints configured")
	}

	return fmt.Errorf("unable to connect to etcd endpoints %v", endpoints)
}
