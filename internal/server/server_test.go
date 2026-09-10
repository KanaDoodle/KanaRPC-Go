package server

import (
	"net"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC-Go/internal/codec"
	"github.com/KanaDoodle/KanaRPC-Go/internal/protocol"
	"github.com/KanaDoodle/KanaRPC-Go/internal/transport"
)

type blockingArgs struct{}
type blockingReply struct {
	OK bool
}

type blockingService struct {
	started chan struct{}
	release chan struct{}
}

func (s *blockingService) Work(_ *blockingArgs, reply *blockingReply) error {
	s.started <- struct{}{}
	<-s.release
	reply.OK = true
	return nil
}

func TestHandleProcessesRequestsConcurrentlyOnOneConnection(t *testing.T) {
	srv, err := NewServer("127.0.0.1:0", WithMaxConcurrentRequests(2))
	if err != nil {
		t.Fatal(err)
	}
	svc := &blockingService{
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	srv.Register("Blocking", svc)

	serverSide, clientSide := net.Pipe()
	serverConn := transport.NewTCPConnection(serverSide)
	clientConn := transport.NewTCPConnection(clientSide)
	handleDone := make(chan struct{})
	go func() {
		srv.Handle(serverConn)
		close(handleDone)
	}()

	jsonCodec, err := codec.New(codec.JSON)
	if err != nil {
		t.Fatal(err)
	}
	body, err := jsonCodec.Marshal(&blockingArgs{})
	if err != nil {
		t.Fatal(err)
	}
	for requestID := uint64(1); requestID <= 2; requestID++ {
		msg := &protocol.Message{
			Header: &protocol.Header{
				RequestID:   requestID,
				ServiceName: "Blocking",
				MethodName:  "Work",
				Compression: codec.CompressionNone,
			},
			Body: body,
		}
		if err := clientConn.Write(msg); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 2; i++ {
		select {
		case <-svc.started:
		case <-time.After(time.Second):
			close(svc.release)
			t.Fatal("second request did not start while the first was in flight")
		}
	}
	close(svc.release)

	for i := 0; i < 2; i++ {
		if _, err := clientConn.Read(); err != nil {
			t.Fatal(err)
		}
	}
	_ = clientConn.Close()

	select {
	case <-handleDone:
	case <-time.After(time.Second):
		t.Fatal("Handle() did not exit after the peer closed")
	}
}
