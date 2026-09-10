package transport

import (
	"context"
	"github.com/KanaDoodle/KanaRPC-Go/internal/codec"
	"sync"
	"time"
)

type Future struct {
	done       chan struct{}
	res        []byte
	err        error
	mu         sync.Mutex
	codec      codec.Codec
	completed  bool
	onComplete []func(error)
}

func NewFuture() *Future {
	c, _ := codec.New(codec.JSON)
	return NewFutureWithCodec(c)
}

func NewFutureWithCodec(c codec.Codec) *Future {
	return &Future{
		done:  make(chan struct{}),
		codec: c,
	}
}

func (f *Future) Done(res []byte, err error) {
	f.mu.Lock()
	if f.completed {
		f.mu.Unlock()
		return
	}
	f.res = res
	f.err = err
	f.completed = true
	callbacks := f.onComplete
	f.onComplete = nil
	close(f.done)
	f.mu.Unlock()

	// Callbacks may reenter Done; never invoke user code while holding a lock.
	for _, callback := range callbacks {
		callback(err)
	}
}

func (f *Future) Wait() ([]byte, error) {
	<-f.done
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.res, f.err
}

func (f *Future) OnComplete(fn func(error)) {
	if fn == nil {
		return
	}

	f.mu.Lock()
	if !f.completed {
		f.onComplete = append(f.onComplete, fn)
		f.mu.Unlock()
		return
	}
	err := f.err
	f.mu.Unlock()
	fn(err)
}

func (f *Future) WaitWithContext(ctx context.Context) ([]byte, error) {
	select {
	case <-f.done:
		return f.Wait()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *Future) DoneChan() <-chan struct{} {
	return f.done
}

func (f *Future) GetResult(reply interface{}) error {
	<-f.done
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return f.err
	}

	return f.codec.Unmarshal(f.res, reply)
}

func (f *Future) GetResultWithContext(ctx context.Context, reply interface{}) error {
	select {
	case <-f.done:
		return f.GetResult(reply)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *Future) IsDone() bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}

func (f *Future) WaitWithTimeout(timeout time.Duration) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return f.WaitWithContext(ctx)
}
