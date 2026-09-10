package server

import (
	"errors"
	"github.com/KanaDoodle/KanaRPC-Go/internal/codec"
	"github.com/KanaDoodle/KanaRPC-Go/internal/limiter"
	"github.com/KanaDoodle/KanaRPC-Go/internal/protocol"
	"github.com/KanaDoodle/KanaRPC-Go/internal/transport"
	"log"
	"net"
	"sync"
)

const defaultMaxConcurrentRequests = 256

type Server struct {
	addr       string
	services   map[string]interface{}
	servicesMu sync.RWMutex
	limiter    *limiter.TokenBucket
	listener   net.Listener
	handler    *Handler
	codec      codec.Codec

	conns                 map[*transport.TCPConnection]struct{}
	connsMu               sync.Mutex
	closing               chan struct{}
	closeOnce             sync.Once
	wg                    sync.WaitGroup
	maxConcurrentRequests int
	workerSem             chan struct{}
}

// 这边用了另外一种go规范去创建对象
func mustNewHandler() *Handler {
	h, err := NewHandler(nil, WithHandlerCodec(codec.JSON))
	if err != nil {
		panic(err)
	}
	return h
}

func NewServer(addr string, opts ...ServerOption) (*Server, error) {
	s := &Server{
		addr:                  addr,
		services:              make(map[string]interface{}),
		limiter:               limiter.NewTokenBucket(10000),
		handler:               mustNewHandler(),
		conns:                 make(map[*transport.TCPConnection]struct{}),
		closing:               make(chan struct{}),
		maxConcurrentRequests: defaultMaxConcurrentRequests,
	}

	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}
	s.workerSem = make(chan struct{}, s.maxConcurrentRequests)
	return s, nil
}

func (s *Server) Register(name string, service interface{}) {
	s.servicesMu.Lock()
	defer s.servicesMu.Unlock()
	s.services[name] = service
}

// Handle keeps request reads ordered while dispatching processing to a bounded
// worker set. Responses are correlated by requestID and TCPConnection serializes
// writes, so a single connection can safely carry concurrent in-flight calls.
func (s *Server) Handle(conn *transport.TCPConnection) {
	var requests sync.WaitGroup
	defer func() {
		requests.Wait()
		_ = conn.Close()
	}()

	for {
		// 读取请求
		msg, err := conn.Read()
		if err != nil {
			// 连接被关闭或出错，退出
			return
		}

		select {
		case s.workerSem <- struct{}{}:
		case <-s.closing:
			return
		}

		// Both semaphore and closing may be ready. Recheck under the shutdown
		// lock so a random select choice cannot admit work after shutdown.
		s.connsMu.Lock()
		select {
		case <-s.closing:
			s.connsMu.Unlock()
			<-s.workerSem
			return
		default:
		}
		requests.Add(1)
		s.connsMu.Unlock()
		go func(msg *protocol.Message) {
			defer requests.Done()
			defer func() { <-s.workerSem }()

			if !s.limiter.Allow() {
				resp := &protocol.Message{
					Header: &protocol.Header{
						RequestID:   msg.Header.RequestID,
						Error:       "rate limit exceeded",
						Compression: codec.CompressionGzip,
					},
				}
				_ = conn.Write(resp)
				return
			}

			s.servicesMu.RLock()
			service := s.services[msg.Header.ServiceName]
			s.servicesMu.RUnlock()
			s.handler.Process(conn, msg, service)
		}(msg)
	}
}

func (s *Server) Start() error {
	s.connsMu.Lock()
	select {
	case <-s.closing:
		s.connsMu.Unlock()
		return ErrServerClosed
	default:
	}
	if s.listener != nil {
		s.connsMu.Unlock()
		return ErrServerStarted
	}
	s.connsMu.Unlock()

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.connsMu.Lock()
	select {
	case <-s.closing:
		s.connsMu.Unlock()
		_ = ln.Close()
		return ErrServerClosed
	default:
	}
	if s.listener != nil {
		s.connsMu.Unlock()
		_ = ln.Close()
		return ErrServerStarted
	}
	s.listener = ln
	s.connsMu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.closing:
				return nil
			default:
				return err
			}
		}

		tcpConn := transport.NewTCPConnection(conn)

		s.connsMu.Lock()
		select {
		case <-s.closing:
			s.connsMu.Unlock()
			_ = tcpConn.Close()
			return nil
		default:
		}
		s.conns[tcpConn] = struct{}{}
		// Admission and Add share the shutdown lock: Wait cannot begin while
		// an accepted connection is still being added to the server lifetime.
		s.wg.Add(1)
		s.connsMu.Unlock()

		go func() {
			defer s.wg.Done()
			s.Handle(tcpConn)
			s.connsMu.Lock()
			delete(s.conns, tcpConn)
			s.connsMu.Unlock()
		}()
	}

}

func (s *Server) Close() {
	s.Shutdown()
}

func (s *Server) Shutdown() {
	s.closeOnce.Do(func() {
		s.connsMu.Lock()
		close(s.closing)
		listener := s.listener
		connections := make([]*transport.TCPConnection, 0, len(s.conns))
		for conn := range s.conns {
			connections = append(connections, conn)
		}
		s.connsMu.Unlock()

		if listener != nil {
			_ = listener.Close()
		}

		for _, conn := range connections {
			_ = conn.Close()
		}

		s.wg.Wait()
		log.Println("server shutdown complete")
	})
}

func (s *Server) Addr() net.Addr {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

var ErrInvalidMaxConcurrentRequests = errors.New("max concurrent requests must be positive")
var ErrServerClosed = errors.New("server is closed")
var ErrServerStarted = errors.New("server is already started")
