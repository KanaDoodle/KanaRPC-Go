package client

import (
	"errors"
	"github.com/KanaDoodle/KanaRPC-Go/internal/codec"
	"github.com/KanaDoodle/KanaRPC-Go/internal/limiter"
	"github.com/KanaDoodle/KanaRPC-Go/internal/loadbalance"
	"time"
)

type ClientOption func(*Client) error

func WithClientCodec(t codec.Type) ClientOption {
	return func(c *Client) error {
		cc, err := codec.New(t)
		if err != nil {
			return err
		}
		c.codec = cc
		c.codecType = t
		return nil
	}
}

func WithClientRateLimit(rate int) ClientOption {
	return func(c *Client) error {
		if rate <= 0 {
			return errors.New("client rate limit must be positive")
		}
		c.limiter = limiter.NewTokenBucket(rate)
		return nil
	}
}

func WithClientTimeout(d time.Duration) ClientOption {
	return func(c *Client) error {
		c.timeout = d
		return nil
	}
}

func WithClientLoadBalancer(lb loadbalance.LoadBalancer) ClientOption {
	return func(c *Client) error {
		if _, ok := lb.(interface {
			NewBalancer() loadbalance.LoadBalancer
		}); !ok {
			return errors.New("load balancer must provide NewBalancer for per-service state")
		}
		c.lb = lb
		return nil
	}
}
