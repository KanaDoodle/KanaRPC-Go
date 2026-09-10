package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC-Go/internal/protocol"
)

type auditPartialWriteConn struct {
	mu      sync.Mutex
	closed  bool
	writes  int
	entered chan struct{}
	release chan struct{}
}

func (c *auditPartialWriteConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *auditPartialWriteConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, net.ErrClosed
	}
	c.writes++
	first := c.writes == 1
	c.mu.Unlock()
	if first {
		close(c.entered)
		<-c.release
		return 1, io.ErrUnexpectedEOF
	}
	return len(p), nil
}
func (c *auditPartialWriteConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}
func (*auditPartialWriteConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*auditPartialWriteConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*auditPartialWriteConn) SetDeadline(time.Time) error      { return nil }
func (*auditPartialWriteConn) SetReadDeadline(time.Time) error  { return nil }
func (*auditPartialWriteConn) SetWriteDeadline(time.Time) error { return nil }

func TestAuditPartialWriteClosesBeforeNextWriter(t *testing.T) {
	raw := &auditPartialWriteConn{entered: make(chan struct{}), release: make(chan struct{})}
	conn := NewTCPConnection(raw)
	defer conn.Close()
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- conn.Write(&protocol.Message{Header: &protocol.Header{}}) }()
	<-raw.entered
	go func() { second <- conn.Write(&protocol.Message{Header: &protocol.Header{}}) }()
	close(raw.release)
	if err := <-first; !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("first error = %v", err)
	}
	if err := <-second; !errors.Is(err, net.ErrClosed) {
		t.Fatalf("writer continued on a partially written frame: second error = %v", err)
	}
	raw.mu.Lock()
	defer raw.mu.Unlock()
	if raw.writes != 1 {
		t.Fatalf("corrupted stream received %d writes, want only the first", raw.writes)
	}
}

func TestAuditCanceledQueuedWriterDoesNotCloseConnection(t *testing.T) {
	c, peer := auditPipeClient(t)
	type result struct {
		future *Future
		err    error
	}
	first := make(chan result, 1)
	go func() {
		f, err := c.SendAsync(&protocol.Message{Header: &protocol.Header{}})
		first <- result{f, err}
	}()
	deadline := time.Now().Add(time.Second)
	for len(c.conn.writeMu) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(c.conn.writeMu) == 0 {
		t.Fatal("first writer did not enter the write gate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() {
		_, err := c.SendAsyncContext(ctx, &protocol.Message{Header: &protocol.Header{}})
		second <- err
	}()
	select {
	case err := <-second:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queued writer = %v, want deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued writer ignored cancellation")
	}
	if atomic.LoadInt32(&c.closed) != 0 {
		t.Fatal("canceling a writer before it sends bytes closed the shared connection")
	}
	request, err := peer.Read()
	if err != nil {
		t.Fatal(err)
	}
	r := <-first
	if r.err != nil {
		t.Fatal(r.err)
	}
	if err := peer.Write(&protocol.Message{Header: request.Header, Body: []byte(`1`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.future.WaitWithTimeout(time.Second); err != nil {
		t.Fatalf("queued cancellation affected the first request: %v", err)
	}
}

type auditCancelAfterWriteConn struct {
	net.Conn
	cancel context.CancelFunc
}

func (c *auditCancelAfterWriteConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.cancel()
	return n, err
}

func TestAuditCancellationAfterFullWriteKeepsStreamUsable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	local, remote := net.Pipe()
	c := &TCPClient{conn: NewTCPConnection(&auditCancelAfterWriteConn{Conn: local, cancel: cancel})}
	peer := NewTCPConnection(remote)
	go c.readLoop()
	defer c.Close()
	defer peer.Close()
	future, _ := auditSendRequest(t, c, peer, ctx)
	if _, err := future.WaitWithTimeout(time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation after a complete write = %v, want canceled Future", err)
	}
	if atomic.LoadInt32(&c.closed) != 0 {
		t.Fatal("a completely written canceled request corrupted the connection")
	}
	next, request := auditSendRequest(t, c, peer, context.Background())
	if err := peer.Write(&protocol.Message{Header: request.Header, Body: []byte(`2`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := next.WaitWithTimeout(time.Second); err != nil {
		t.Fatalf("cancellation deadline leaked into the next write: %v", err)
	}
}
