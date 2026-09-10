package transport

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var ErrPoolClosed = errors.New("connection pool closed")

type ConnectionPool struct {
	addr string

	maxActive int

	conns []*TCPClient
	mu    sync.Mutex

	closed  bool
	next    int
	dialing int
	changed chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewConnectionPool(addr string, maxIdle, maxActive int) *ConnectionPool {
	if maxActive < 1 {
		maxActive = 1
	}
	return &ConnectionPool{
		addr:      addr,
		maxActive: maxActive,
		conns:     make([]*TCPClient, 0, maxActive),
	}
}

func (p *ConnectionPool) Acquire(ctx context.Context) (*TCPClient, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrPoolClosed
		}
		if p.changed == nil {
			p.changed = make(chan struct{})
			p.ctx, p.cancel = context.WithCancel(context.Background())
		}
		wasFull := len(p.conns) >= p.maxActive
		alive := p.conns[:0]
		for _, conn := range p.conns {
			if atomic.LoadInt32(&conn.closed) == 0 {
				alive = append(alive, conn)
			}
		}
		clear(p.conns[len(alive):])
		p.conns = alive
		if len(p.conns) > 0 && (wasFull || len(p.conns)+p.dialing >= p.maxActive) {
			idx := p.next % len(p.conns)
			conn := p.conns[idx]
			p.next = (idx + 1) % len(p.conns)
			p.mu.Unlock()
			return conn, nil
		}
		if len(p.conns)+p.dialing >= p.maxActive {
			changed, poolCtx := p.changed, p.ctx
			p.mu.Unlock()
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-poolCtx.Done():
				return nil, ErrPoolClosed
			}
		}
		// Reserve a slot, but never hold the pool lock over network I/O.
		p.dialing++
		poolCtx := p.ctx
		p.mu.Unlock()
		dialCtx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(poolCtx, cancel)
		conn, err := newTCPClientContext(dialCtx, p.addr)
		stop()
		cancel()
		p.mu.Lock()
		p.dialing--
		close(p.changed)
		p.changed = make(chan struct{})
		if p.closed {
			err = ErrPoolClosed
		} else if ctx.Err() != nil {
			err = ctx.Err()
		}
		if err == nil {
			p.conns = append(p.conns, conn)
		}
		p.mu.Unlock()
		if err != nil {
			if conn != nil {
				_ = conn.Close()
			}
			return nil, err
		}
		return conn, nil
	}
}

func (p *ConnectionPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	if p.cancel != nil {
		p.cancel()
	}
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	for _, conn := range conns {
		conn.Close()
	}
}
