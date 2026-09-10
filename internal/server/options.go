package server

import (
	"errors"

	"github.com/KanaDoodle/KanaRPC-Go/internal/codec"
	"github.com/KanaDoodle/KanaRPC-Go/internal/limiter"
)

type HandleOption func(*Handler) error

func WithHandlerCodec(t codec.Type) HandleOption {
	return func(c *Handler) error {
		cc, err := codec.New(t)
		if err != nil {
			return err
		}
		c.codec = cc
		return nil
	}
}

type ServerOption func(*Server) error

func WithServerCodec(t codec.Type) ServerOption {
	return func(c *Server) error {
		cc, err := codec.New(t)
		if err != nil {
			return err
		}
		c.codec = cc
		c.handler.codec = cc
		return nil
	}
}

func WithMaxConcurrentRequests(n int) ServerOption {
	return func(s *Server) error {
		if n <= 0 {
			return ErrInvalidMaxConcurrentRequests
		}
		s.maxConcurrentRequests = n
		return nil
	}
}

func WithServerRateLimit(rate int) ServerOption {
	return func(s *Server) error {
		if rate <= 0 {
			return errors.New("server rate limit must be positive")
		}
		s.limiter = limiter.NewTokenBucket(rate)
		return nil
	}
}
