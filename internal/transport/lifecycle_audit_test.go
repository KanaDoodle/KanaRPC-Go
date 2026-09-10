package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC-Go/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func auditPipeClient(t *testing.T) (*TCPClient, *TCPConnection) {
	t.Helper()
	local, remote := net.Pipe()
	c := &TCPClient{conn: NewTCPConnection(local)}
	peer := NewTCPConnection(remote)
	go c.readLoop()
	t.Cleanup(func() {
		_ = peer.Close()
		_ = c.Close()
		c.pending.Range(func(key, value interface{}) bool {
			c.pending.Delete(key)
			value.(*Future).Done(nil, net.ErrClosed)
			return true
		})
	})
	return c, peer
}

func auditSendRequest(t *testing.T, c *TCPClient, peer *TCPConnection, ctx context.Context) (*Future, *protocol.Message) {
	t.Helper()
	type result struct {
		future *Future
		err    error
	}
	sent := make(chan result, 1)
	go func() {
		f, err := c.SendAsyncContext(ctx, &protocol.Message{Header: &protocol.Header{}, Body: []byte(`{}`)})
		sent <- result{f, err}
	}()
	request, err := peer.Read()
	if err != nil {
		t.Fatal(err)
	}
	r := <-sent
	if r.err != nil {
		t.Fatal(r.err)
	}
	return r.future, request
}

func TestAuditCloseCompletesPending(t *testing.T) {
	c, peer := auditPipeClient(t)
	future, _ := auditSendRequest(t, c, peer, context.Background())
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := future.WaitWithContext(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("Close left an accepted request pending instead of completing its Future")
	}
	if err == nil {
		t.Fatal("Close completed pending request without a connection error")
	}
	count := 0
	c.pending.Range(func(_, _ interface{}) bool { count++; return true })
	if count != 0 {
		t.Fatalf("Close retained %d pending requests", count)
	}
}

func TestAuditBlockedWriteHonorsContext(t *testing.T) {
	c, peer := auditPipeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	returned := make(chan error, 1)
	go func() {
		_, err := c.SendAsyncContext(ctx, &protocol.Message{Header: &protocol.Header{}, Body: []byte(`{}`)})
		returned <- err
	}()
	<-ctx.Done()
	select {
	case err := <-returned:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("SendAsyncContext error = %v, want context deadline", err)
		}
	case <-time.After(200 * time.Millisecond):
		_ = peer.Close()
		<-returned
		t.Fatal("SendAsyncContext stayed blocked in Write after its deadline")
	}
}

func TestAuditAcquireRejectsCanceledContext(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	pool := NewConnectionPool(listener.Addr().String(), 0, 1)
	defer pool.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	conn, err := pool.Acquire(ctx)
	if conn != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire(canceled context) = (%v, %v), want (nil, context.Canceled)", conn != nil, err)
	}
}

func TestAuditAcquireKeepsHealthyConnectionAfterDeadEntry(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	healthy, _ := auditPipeClient(t)
	pool := &ConnectionPool{addr: addr, maxActive: 2, conns: []*TCPClient{{closed: 1}, healthy}}
	defer pool.Close()
	got, err := pool.Acquire(context.Background())
	if err != nil || got != healthy {
		t.Fatalf("Acquire skipped the surviving healthy connection: gotHealthy=%v err=%v", got == healthy, err)
	}
}

func TestAuditFutureCallbackCanCompleteAgain(t *testing.T) {
	future := NewFuture()
	returned := make(chan struct{})
	future.OnComplete(func(error) { future.Done(nil, errors.New("duplicate")) })
	go func() {
		future.Done([]byte(`{}`), nil)
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Future.Done deadlocked when its callback attempted duplicate completion")
	}
}

func TestAuditLateResponseDoesNotCompleteAnotherRequest(t *testing.T) {
	c, peer := auditPipeClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	first, firstRequest := auditSendRequest(t, c, peer, ctx)
	cancel()
	if _, err := first.WaitWithTimeout(time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request error = %v", err)
	}
	second, secondRequest := auditSendRequest(t, c, peer, context.Background())
	if err := peer.Write(&protocol.Message{Header: firstRequest.Header, Body: []byte(`"late"`)}); err != nil {
		t.Fatal(err)
	}
	if err := peer.Write(&protocol.Message{Header: secondRequest.Header, Body: []byte(`"current"`)}); err != nil {
		t.Fatal(err)
	}
	body, err := second.WaitWithTimeout(time.Second)
	if err != nil || string(body) != `"current"` {
		t.Fatalf("second request = %q, %v; late response must be discarded", body, err)
	}
}

func TestAuditFutureUsesRequestCodec(t *testing.T) {
	c, peer := auditPipeClient(t)
	body, err := proto.Marshal(wrapperspb.Int64(42))
	if err != nil {
		t.Fatal(err)
	}
	sent := make(chan *Future, 1)
	go func() {
		future, _ := c.SendAsync(&protocol.Message{Header: &protocol.Header{CodecType: protocol.CodecTypeProto}, Body: body})
		sent <- future
	}()
	request, err := peer.Read()
	if err != nil {
		t.Fatal(err)
	}
	future := <-sent
	if future == nil {
		t.Fatal("SendAsync failed")
	}
	if err := peer.Write(&protocol.Message{Header: request.Header, Body: body}); err != nil {
		t.Fatal(err)
	}
	var reply wrapperspb.Int64Value
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := future.GetResultWithContext(ctx, &reply); err != nil || reply.Value != 42 {
		t.Fatalf("protobuf request Future decoded as wrong codec: value=%d err=%v", reply.Value, err)
	}
}

func TestAuditSynchronousCallbackBlocksReaderUntilReleased(t *testing.T) {
	c, peer := auditPipeClient(t)
	first, firstRequest := auditSendRequest(t, c, peer, context.Background())
	second, secondRequest := auditSendRequest(t, c, peer, context.Background())
	entered, release := make(chan struct{}), make(chan struct{})
	first.OnComplete(func(error) { close(entered); <-release })
	if err := peer.Write(&protocol.Message{Header: firstRequest.Header, Body: []byte(`1`)}); err != nil {
		close(release)
		t.Fatal(err)
	}
	<-entered
	writeReturned := make(chan error, 1)
	go func() {
		writeReturned <- peer.Write(&protocol.Message{Header: secondRequest.Header, Body: []byte(`2`)})
	}()
	select {
	case <-second.DoneChan():
		close(release)
		t.Fatal("callback scheduling changed: revisit this documented synchronous-callback risk")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-writeReturned; err != nil {
		t.Fatal(err)
	}
	if body, err := second.WaitWithTimeout(time.Second); err != nil || string(body) != "2" {
		t.Fatalf("reader did not resume after callback release: %q, %v", body, err)
	}
	t.Log("confirmed: a synchronous OnComplete callback stalls unrelated responses until it returns")
}
