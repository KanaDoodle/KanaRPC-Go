package registry

import (
	"context"
	"errors"
	"fmt"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"math/rand/v2"
	"time"
)

type registrationState struct {
	registered bool
	expires    time.Time
}

// Ready is false during startup, after detected keepalive loss, after the last
// confirmed lease TTL expires, and after Close. Recovery restores it.
func (r *Registry) Ready() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed || len(r.registrations) == 0 {
		return false
	}
	for _, v := range r.registrations {
		if !v.registered || !time.Now().Before(v.expires) {
			return false
		}
	}
	return true
}
func (r *Registry) Register(service string, ins Instance, ttl int64) error {
	if service == "" || ins.Addr == "" || ttl < 1 || ttl > 86400 {
		return errors.New("invalid registration")
	}
	key := fmt.Sprintf("%s%s/%s", r.prefix, service, ins.Addr)
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return errors.New("registry closed")
	}
	if r.registrations == nil {
		r.registrations = make(map[string]registrationState)
	}
	if _, ok := r.registrations[key]; ok {
		r.mu.Unlock()
		return errors.New("registration already started")
	}
	r.registrations[key] = registrationState{}
	r.wg.Add(1)
	r.mu.Unlock()
	defer r.wg.Done()
	id, ch, stop, err := r.obtainRegistration(key, ins.Addr, ttl)
	if err != nil {
		r.mu.Lock()
		delete(r.registrations, key)
		r.mu.Unlock()
		return err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		stop()
		r.cleanupLease(id)
		return errors.New("registry closed")
	}
	r.registrations[key] = registrationState{true, time.Now().Add(time.Duration(ttl) * time.Second)}
	r.wg.Add(1)
	r.mu.Unlock()
	go func() { defer r.wg.Done(); r.maintainRegistration(key, ins.Addr, ttl, id, ch, stop) }()
	return nil
}
func (r *Registry) obtainRegistration(key, addr string, ttl int64) (clientv3.LeaseID, <-chan *clientv3.LeaseKeepAliveResponse, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()
	lease, err := r.client.Grant(ctx, ttl)
	if err != nil {
		return 0, nil, nil, err
	}
	r.mu.Lock()
	if r.leases == nil {
		r.leases = make(map[clientv3.LeaseID]struct{})
	}
	r.leases[lease.ID] = struct{}{}
	r.mu.Unlock()
	kaCtx, stop := context.WithCancel(r.ctx)
	success := false
	defer func() {
		if !success {
			stop()
			r.cleanupLease(lease.ID)
		}
	}()
	if _, err = r.client.Put(ctx, key, addr, clientv3.WithLease(lease.ID)); err != nil {
		return 0, nil, nil, err
	}
	ch, err := r.client.KeepAlive(kaCtx, lease.ID)
	if err != nil {
		return 0, nil, nil, err
	}
	if ch == nil {
		return 0, nil, nil, errors.New("nil keepalive channel")
	}
	success = true
	return lease.ID, ch, stop, nil
}
func (r *Registry) cleanupLease(id clientv3.LeaseID) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := r.client.Revoke(ctx, id)
	if err == nil || errors.Is(err, rpctypes.ErrLeaseNotFound) {
		r.mu.Lock()
		delete(r.leases, id)
		r.mu.Unlock()
	}
}
func (r *Registry) maintainRegistration(key, addr string, ttl int64, id clientv3.LeaseID, ch <-chan *clientv3.LeaseKeepAliveResponse, stop context.CancelFunc) {
	defer func() { stop(); r.mu.Lock(); r.registrations[key] = registrationState{}; r.mu.Unlock() }()
	for {
		timer := time.NewTimer(time.Duration(ttl) * time.Second)
	alive:
		for {
			select {
			case <-r.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				break alive
			case resp, ok := <-ch:
				if !ok || resp == nil || resp.TTL <= 0 {
					break alive
				}
				r.mu.Lock()
				r.registrations[key] = registrationState{true, time.Now().Add(time.Duration(resp.TTL) * time.Second)}
				r.mu.Unlock()
				timer.Reset(time.Duration(resp.TTL) * time.Second)
			}
		}
		timer.Stop()
		stop()
		r.mu.Lock()
		r.registrations[key] = registrationState{}
		r.mu.Unlock()
		r.cleanupLease(id)
		delay := 100 * time.Millisecond
		for {
			wait := time.NewTimer(delay + time.Duration(rand.Int64N(int64(delay/2)+1)))
			select {
			case <-r.ctx.Done():
				wait.Stop()
				return
			case <-wait.C:
			}
			var err error
			id, ch, stop, err = r.obtainRegistration(key, addr, ttl)
			if err == nil {
				r.mu.Lock()
				r.registrations[key] = registrationState{true, time.Now().Add(time.Duration(ttl) * time.Second)}
				r.mu.Unlock()
				break
			}
			// Keep a valid cancellation function on failed attempts for the defer.
			stop = func() {}
			if delay < 5*time.Second {
				delay *= 2
				if delay > 5*time.Second {
					delay = 5 * time.Second
				}
			}
		}
	}
}
