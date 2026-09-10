package registry

import (
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sync/atomic"
	"testing"
	"time"
)

type repairLease struct {
	clientv3.Lease
	calls  atomic.Int32
	lost   chan struct{}
	resume chan struct{}
}

func (l *repairLease) KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	if l.calls.Add(1) == 1 {
		ch := make(chan *clientv3.LeaseKeepAliveResponse)
		go func() {
			select {
			case <-ctx.Done():
			case <-l.lost:
			}
			close(ch)
		}()
		return ch, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.resume:
	}
	return l.Lease.KeepAlive(ctx, id)
}
func TestRepairRegistrationRecovery(t *testing.T) {
	for _, mode := range []string{"revoke", "closed-channel"} {
		t.Run(mode, func(t *testing.T) {
			r, cli := auditEtcdRegistry(t)
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			var fake *repairLease
			if mode == "closed-channel" {
				fake = &repairLease{Lease: r.client.Lease, lost: make(chan struct{}), resume: make(chan struct{})}
				r.client.Lease = fake
			}
			if e := r.Register("svc", Instance{Addr: "a"}, 3); e != nil {
				t.Fatal(e)
			}
			key := r.prefix + "svc/a"
			before, e := cli.Get(ctx, key)
			if e != nil || len(before.Kvs) != 1 {
				t.Fatal(e)
			}
			lease := before.Kvs[0].Lease
			if mode == "revoke" {
				if _, e = cli.Revoke(ctx, clientv3.LeaseID(lease)); e != nil {
					t.Fatal(e)
				}
			} else {
				close(fake.lost)
				time.Sleep(150 * time.Millisecond)
				if ready, ok := any(r).(interface{ Ready() bool }); !ok || ready.Ready() {
					t.Error("F09: registration loss not reflected in readiness")
				}
				close(fake.resume)
			}
			for ctx.Err() == nil {
				after, e := cli.Get(ctx, key)
				if e == nil && len(after.Kvs) == 1 && after.Kvs[0].Lease != lease {
					if ready, ok := any(r).(interface{ Ready() bool }); ok && ready.Ready() {
						xs, e := r.Discover("svc")
						if e == nil && len(xs) == 1 {
							return
						}
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			t.Fatal("F09: registration did not recover with a new lease and ready state")
		})
	}
}
