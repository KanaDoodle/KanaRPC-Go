package server

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC-Go/internal/codec"
	"github.com/KanaDoodle/KanaRPC-Go/internal/protocol"
	"github.com/KanaDoodle/KanaRPC-Go/internal/transport"
)

func TestServerAddrCanBeReadWhileStarting(t *testing.T) {
	srv, err := NewServer("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	startDone := make(chan error, 1)
	go func() { startDone <- srv.Start() }()
	deadline := time.Now().Add(time.Second)
	for srv.Addr() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if srv.Addr() == nil {
		t.Fatal("server did not start")
	}
	srv.Shutdown()
	select {
	case err := <-startDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not stop")
	}
}

func TestServerCannotStartAfterShutdown(t *testing.T) {
	srv, err := NewServer("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.Shutdown()
	startDone := make(chan error, 1)
	go func() { startDone <- srv.Start() }()
	select {
	case err := <-startDone:
		if err == nil {
			t.Fatal("Start after shutdown must report a closed server")
		}
	case <-time.After(100 * time.Millisecond):
		// Release the old implementation's leaked listener before failing.
		if srv.listener != nil {
			_ = srv.listener.Close()
		}
		<-startDone
		t.Fatal("Start after shutdown opened a listener and blocked")
	}
}

type auditEchoService struct{}

func (*auditEchoService) Work(_ *blockingArgs, reply *blockingReply) error {
	reply.OK = true
	return nil
}

func TestServerRegisterConcurrentWithRequests(t *testing.T) {
	srv, err := NewServer("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.Register("Echo", &auditEchoService{})
	serverSide, clientSide := net.Pipe()
	serverConn := transport.NewTCPConnection(serverSide)
	clientConn := transport.NewTCPConnection(clientSide)
	done := make(chan struct{})
	go func() { srv.Handle(serverConn); close(done) }()
	defer func() { _ = clientConn.Close(); <-done; srv.Close() }()
	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		for i := 0; i < 200; i++ {
			srv.Register(fmt.Sprintf("Other-%d", i), &auditEchoService{})
			time.Sleep(time.Microsecond)
		}
	}()
	for i := uint64(1); i <= 200; i++ {
		if err := clientConn.Write(&protocol.Message{Header: &protocol.Header{RequestID: i, ServiceName: "Echo", MethodName: "Work", Compression: codec.CompressionNone}, Body: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
		msg, err := clientConn.Read()
		if err != nil {
			t.Fatal(err)
		}
		if msg.Header.Error != "" {
			t.Fatal(msg.Header.Error)
		}
	}
	writers.Wait()
}

func TestServerShutdownConcurrentWithAccept(t *testing.T) {
	for iteration := 0; iteration < 30; iteration++ {
		srv, err := NewServer("127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		startDone := make(chan error, 1)
		go func() { startDone <- srv.Start() }()
		deadline := time.Now().Add(time.Second)
		for srv.Addr() == nil && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if srv.Addr() == nil {
			t.Fatal("server did not start")
		}
		addr := srv.Addr().String()
		ready := make(chan struct{})
		accepted := make(chan net.Conn, 12)
		var dialers sync.WaitGroup
		for i := 0; i < cap(accepted); i++ {
			dialers.Add(1)
			go func() {
				defer dialers.Done()
				<-ready
				if conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
					accepted <- conn
				}
			}()
		}
		shutdownDone := make(chan struct{})
		go func() { <-ready; srv.Shutdown(); close(shutdownDone) }()
		close(ready)
		dialers.Wait()
		close(accepted)
		var connections []net.Conn
		for conn := range accepted {
			connections = append(connections, conn)
		}
		select {
		case <-shutdownDone:
		case <-time.After(time.Second):
			for _, conn := range connections {
				_ = conn.Close()
			}
			t.Fatalf("iteration %d: shutdown missed an accepted connection", iteration)
		}
		for _, conn := range connections {
			_ = conn.Close()
		}
		select {
		case err := <-startDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("accept loop did not stop")
		}
		srv.connsMu.Lock()
		remaining := len(srv.conns)
		srv.connsMu.Unlock()
		if remaining != 0 {
			t.Fatalf("shutdown left %d tracked connections", remaining)
		}
	}
}

type auditCountingService struct{ calls atomic.Int32 }

func (s *auditCountingService) Work(_ *blockingArgs, reply *blockingReply) error {
	s.calls.Add(1)
	reply.OK = true
	return nil
}

func TestServerHandleAfterShutdownRejectsRequests(t *testing.T) {
	var total int32
	for i := 0; i < 24; i++ {
		srv, err := NewServer("127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		svc := &auditCountingService{}
		srv.Register("Count", svc)
		srv.Shutdown()
		serverSide, clientSide := net.Pipe()
		clientConn := transport.NewTCPConnection(clientSide)
		done := make(chan struct{})
		go func() { srv.Handle(transport.NewTCPConnection(serverSide)); close(done) }()
		_ = clientConn.Write(&protocol.Message{Header: &protocol.Header{RequestID: 1, ServiceName: "Count", MethodName: "Work", Compression: codec.CompressionNone}, Body: []byte(`{}`)})
		_ = clientConn.Close()
		<-done
		total += svc.calls.Load()
	}
	if total != 0 {
		t.Fatalf("already-shut-down servers executed %d requests", total)
	}
}

func TestShutdownWaitsForActiveWorker(t *testing.T) {
	srv, err := NewServer("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	svc := &blockingService{started: make(chan struct{}, 1), release: make(chan struct{})}
	srv.Register("Blocking", svc)
	serverSide, clientSide := net.Pipe()
	serverConn := transport.NewTCPConnection(serverSide)
	clientConn := transport.NewTCPConnection(clientSide)
	srv.conns[serverConn] = struct{}{}
	srv.wg.Add(1)
	go func() {
		defer srv.wg.Done()
		srv.Handle(serverConn)
		srv.connsMu.Lock()
		delete(srv.conns, serverConn)
		srv.connsMu.Unlock()
	}()
	if err := clientConn.Write(&protocol.Message{Header: &protocol.Header{RequestID: 1, ServiceName: "Blocking", MethodName: "Work", Compression: codec.CompressionNone}, Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	<-svc.started
	shutdownDone := make(chan struct{})
	go func() { srv.Shutdown(); close(shutdownDone) }()
	response := make(chan error, 1)
	go func() { _, err := clientConn.Read(); response <- err }()
	select {
	case err := <-response:
		t.Logf("shutdown closes the client connection before the active worker completes: %v", err)
	case <-time.After(100 * time.Millisecond):
		t.Log("connection remains open while worker is active")
	}
	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned before active worker completed")
	default:
	}
	close(svc.release)
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not return after worker completed")
	}
	_ = clientConn.Close()
}
