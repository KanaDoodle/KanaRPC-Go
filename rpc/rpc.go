// Package rpc is the supported public boundary of KanaRPC. Wire, connection
// pool, breaker and registry implementation types remain internal.
package rpc

import (
	"context"
	"errors"
	"github.com/KanaDoodle/KanaRPC-Go/internal/client"
	"github.com/KanaDoodle/KanaRPC-Go/internal/loadbalance"
	"github.com/KanaDoodle/KanaRPC-Go/internal/registry"
	"github.com/KanaDoodle/KanaRPC-Go/internal/server"
	"net"
	"time"
)

type Registry struct{ inner *registry.Registry }

func NewRegistry(endpoints []string) (*Registry, error) {
	r, err := registry.NewRegistry(endpoints)
	if err != nil {
		return nil, err
	}
	return &Registry{r}, nil
}
func (r *Registry) Register(service, address string, leaseTTLSeconds int64) error {
	return r.inner.Register(service, registry.Instance{Addr: address}, leaseTTLSeconds)
}
func (r *Registry) DiscoverContext(ctx context.Context, service string) ([]string, error) {
	xs, err := r.inner.DiscoverContext(ctx, service)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		out = append(out, x.Addr)
	}
	return out, nil
}
func (r *Registry) Ready() bool  { return r != nil && r.inner.Ready() }
func (r *Registry) Close() error { return r.inner.Close() }

type Selection string

const (
	RoundRobin Selection = "ROUND_ROBIN"
	Random     Selection = "RANDOM"
)

type ClientConfig struct {
	Timeout   time.Duration
	Selection Selection
}
type Client struct{ inner *client.Client }

func NewClient(r *Registry, cfg ClientConfig) (*Client, error) {
	if r == nil {
		return nil, errors.New("registry required")
	}
	if cfg.Timeout < 0 {
		return nil, errors.New("timeout must be positive")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	opts := []client.ClientOption{client.WithClientTimeout(cfg.Timeout)}
	switch cfg.Selection {
	case "", RoundRobin:
	case Random:
		opts = append(opts, client.WithClientLoadBalancer(loadbalance.NewRandom()))
	default:
		return nil, errors.New("unsupported selection")
	}
	c, err := client.NewClient(r.inner, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{c}, nil
}

// Invoke performs one remote call. Context bounds discovery/dial/write/wait;
// caller cancellation is not a remote server cancellation protocol.
func (c *Client) Invoke(ctx context.Context, service, method string, request, response any) error {
	return c.inner.Invoke(ctx, service, method, request, response)
}
func (c *Client) Close() { c.inner.Close() }

type ServerConfig struct {
	Address               string
	MaxConcurrentRequests int
}
type Server struct{ inner *server.Server }

func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Address == "" {
		return nil, errors.New("server address required")
	}
	opts := []server.ServerOption{}
	if cfg.MaxConcurrentRequests != 0 {
		opts = append(opts, server.WithMaxConcurrentRequests(cfg.MaxConcurrentRequests))
	}
	s, err := server.NewServer(cfg.Address, opts...)
	if err != nil {
		return nil, err
	}
	return &Server{s}, nil
}

// Register uses methods of the form func(*Request, *Response) error.
func (s *Server) Register(name string, handler any) { s.inner.Register(name, handler) }
func (s *Server) Start() error                      { return s.inner.Start() }
func (s *Server) Addr() net.Addr                    { return s.inner.Addr() }
func (s *Server) Shutdown()                         { s.inner.Shutdown() }
