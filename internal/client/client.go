package client

import (
	"context"
	"errors"
	"github.com/KanaDoodle/KanaRPC-Go/internal/breaker"
	"github.com/KanaDoodle/KanaRPC-Go/internal/codec"
	"github.com/KanaDoodle/KanaRPC-Go/internal/limiter"
	"github.com/KanaDoodle/KanaRPC-Go/internal/loadbalance"
	"github.com/KanaDoodle/KanaRPC-Go/internal/protocol"
	"github.com/KanaDoodle/KanaRPC-Go/internal/registry"
	"github.com/KanaDoodle/KanaRPC-Go/internal/transport"
	"sync"
	"time"
)

type Client struct {
	reg       *registry.Registry
	lb        loadbalance.LoadBalancer
	balancers map[string]loadbalance.LoadBalancer
	limiter   *limiter.TokenBucket
	timeout   time.Duration
	codec     codec.Codec
	codecType codec.Type
	breaker   sync.Map // map[string]*CircuitBreaker

	pools  sync.Map // map[string]*transport.ConnectionPool
	mu     sync.Mutex
	closed bool
}

var ErrClientClosed = errors.New("client closed")

func NewClient(reg *registry.Registry, opts ...ClientOption) (*Client, error) {
	c := &Client{
		reg:       reg,
		codecType: codec.JSON,
		lb:        &loadbalance.RoundRobin{},
		limiter:   limiter.NewTokenBucket(10000),
		timeout:   5 * time.Second,
	}
	defaultCodec, err := codec.New(codec.JSON)
	if err != nil {
		return nil, err
	}
	c.codec = defaultCodec
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *Client) InvokeAsync(ctx context.Context, service string, method string, args interface{}) (*transport.Future, error) {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, ErrClientClosed
	}
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	if err := callCtx.Err(); err != nil {
		cancel()
		return nil, err
	}
	// Successful asynchronous calls transfer cancellation to the Future.
	transferred := false
	defer func() {
		if !transferred {
			cancel()
		}
	}()

	if !c.limiter.Allow() {
		return nil, errors.New("rate limit exceeded")
	}

	// Encode before choosing an instance: invalid local input is not a backend fault.
	body, err := c.codec.Marshal(args)
	if err != nil {
		return nil, err
	}
	addr, finish, err := c.selectInstance(callCtx, service)
	if err != nil {
		return nil, err
	}

	pool, err := c.getPool(addr)
	if err != nil {
		finish(context.Canceled)
		return nil, err
	}

	conn, err := pool.Acquire(callCtx)
	if err != nil {
		finish(err)
		return nil, err
	}

	req := &protocol.Message{
		Header: &protocol.Header{
			ServiceName: service,
			MethodName:  method,
			Compression: codec.CompressionGzip,
			CodecType:   protocol.CodecType(c.codecType),
		},
		Body: body,
	}
	future, err := conn.SendAsyncContext(callCtx, req)
	if err != nil {
		finish(err)
		return nil, err
	}

	future.OnComplete(func(err error) {
		cancel()
		finish(err)
	})
	transferred = true

	return future, nil
}

// 同步接口 = 异步 + 等待
func (c *Client) Invoke(ctx context.Context, service string, method string, args interface{}, reply interface{}) error {

	future, err := c.InvokeAsync(ctx, service, method, args)
	if err != nil {
		return err
	}

	return future.GetResultWithContext(ctx, reply)
}

func (c *Client) getPool(addr string) (*transport.ConnectionPool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClientClosed
	}
	if pool, ok := c.pools.Load(addr); ok {
		return pool.(*transport.ConnectionPool), nil
	}

	newPool := transport.NewConnectionPool(addr, 0, 1)
	actual, _ := c.pools.LoadOrStore(addr, newPool)
	return actual.(*transport.ConnectionPool), nil
}

func (c *Client) getAddr(service string) (string, error) {
	return c.getAddrContext(context.Background(), service)
}

func (c *Client) getAddrContext(ctx context.Context, service string) (string, error) {
	if c.reg == nil {
		return "", errors.New("registry not configured")
	}

	instances, err := c.reg.DiscoverContext(ctx, service)
	if err != nil {
		return "", err
	}

	if len(instances) == 0 {
		return "", errors.New("no instance available")
	}

	lb, err := c.serviceBalancer(service)
	if err != nil {
		return "", err
	}
	instance := lb.Select(instances)
	return instance.Addr, nil
}

func (c *Client) Close() {
	c.mu.Lock()
	c.closed = true
	c.balancers = nil
	var pools []*transport.ConnectionPool
	c.pools.Range(func(key, value interface{}) bool {
		pools = append(pools, value.(*transport.ConnectionPool))
		return true
	})
	c.mu.Unlock()
	for _, pool := range pools {
		pool.Close()
	}
}

func (c *Client) getBreaker(service, addr string) *breaker.CircuitBreaker {

	key := service + "|" + addr

	if val, ok := c.breaker.Load(key); ok {
		return val.(*breaker.CircuitBreaker)
	}

	newBreaker := breaker.NewCircuitBreaker(
		10,
		0.6,           // 60% 错误率熔断
		5*time.Second, // 熔断 5 秒
	)

	actual, _ := c.breaker.LoadOrStore(key, newBreaker)

	return actual.(*breaker.CircuitBreaker)
}

var ErrNoUsableInstance = errors.New("no instance available: all circuit breakers unavailable")

func (c *Client) selectInstance(ctx context.Context, service string) (string, func(error), error) {
	if c.reg == nil {
		return "", nil, errors.New("registry not configured")
	}
	instances, err := c.reg.DiscoverContext(ctx, service)
	if err != nil {
		return "", nil, err
	}
	candidates := make([]registry.Instance, 0, len(instances))
	for _, ins := range instances {
		if c.getBreaker(service, ins.Addr).Eligible() {
			candidates = append(candidates, ins)
		}
	}
	// At most one admission attempt per discovered address. No remote call is
	// retried: reselection only handles an admission race before transmission.
	for len(candidates) > 0 {
		if err := ctx.Err(); err != nil {
			return "", nil, err
		}
		lb, err := c.serviceBalancer(service)
		if err != nil {
			return "", nil, err
		}
		chosen := lb.Select(candidates)
		if chosen.Addr == "" {
			return "", nil, ErrNoUsableInstance
		}
		finish, ok := c.getBreaker(service, chosen.Addr).Acquire()
		if ok {
			return chosen.Addr, finish, nil
		}
		rest := candidates[:0]
		for _, ins := range candidates {
			if ins.Addr != chosen.Addr {
				rest = append(rest, ins)
			}
		}
		if len(rest) == len(candidates) {
			return "", nil, ErrNoUsableInstance
		}
		candidates = rest
	}
	return "", nil, ErrNoUsableInstance
}

// Each service owns its selection sequence. Entries live for the Client lifetime
// and are released by Close; closed clients cannot repopulate the map.
func (c *Client) serviceBalancer(service string) (loadbalance.LoadBalancer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClientClosed
	}
	if c.balancers == nil {
		c.balancers = make(map[string]loadbalance.LoadBalancer)
	}
	if lb := c.balancers[service]; lb != nil {
		return lb, nil
	}
	factory, ok := c.lb.(interface {
		NewBalancer() loadbalance.LoadBalancer
	})
	if !ok {
		return nil, errors.New("load balancer must provide independent service state")
	}
	lb := factory.NewBalancer()
	c.balancers[service] = lb
	return lb, nil
}
