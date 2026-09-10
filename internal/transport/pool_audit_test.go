package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
)

func auditListeningPeer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		var accepted []net.Conn
		defer func() {
			for _, conn := range accepted {
				_ = conn.Close()
			}
		}()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted = append(accepted, conn)
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); <-done })
	return listener.Addr().String()
}

func TestAuditConcurrentAcquireRespectsPoolLimit(t *testing.T) {
	pool := NewConnectionPool(auditListeningPeer(t), 0, 4)
	defer pool.Close()
	start := make(chan struct{})
	results := make(chan *TCPClient, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(results); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			conn, err := pool.Acquire(context.Background())
			if err != nil {
				t.Errorf("Acquire: %v", err)
			}
			results <- conn
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	unique := make(map[*TCPClient]bool)
	for conn := range results {
		if conn != nil {
			unique[conn] = true
		}
	}
	if len(unique) == 0 || len(unique) > 4 {
		t.Fatalf("concurrent callers received %d distinct connections for maxActive=4", len(unique))
	}
}

func TestAuditAcquireRacingCloseLeavesNoLiveConnection(t *testing.T) {
	addr := auditListeningPeer(t)
	for round := 0; round < 20; round++ {
		pool := NewConnectionPool(addr, 0, 4)
		start := make(chan struct{})
		results := make(chan *TCPClient, 16)
		var wg sync.WaitGroup
		for i := 0; i < cap(results); i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				conn, err := pool.Acquire(context.Background())
				if err != nil && !errors.Is(err, ErrPoolClosed) {
					t.Errorf("Acquire racing Close: %v", err)
				}
				results <- conn
			}()
		}
		wg.Add(1)
		go func() { defer wg.Done(); <-start; pool.Close() }()
		close(start)
		wg.Wait()
		close(results)
		for conn := range results {
			if conn != nil && atomic.LoadInt32(&conn.closed) == 0 {
				t.Fatal("Acquire published a live connection after pool Close")
			}
		}
		pool.mu.Lock()
		retained, dialing := len(pool.conns), pool.dialing
		pool.mu.Unlock()
		if retained != 0 || dialing != 0 {
			t.Fatalf("Close retained %d connections and %d dials", retained, dialing)
		}
	}
}
