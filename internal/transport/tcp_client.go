package transport

import (
	"context"
	"errors"
	"github.com/KanaDoodle/KanaRPC-Go/internal/codec"
	"github.com/KanaDoodle/KanaRPC-Go/internal/protocol"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type TCPClient struct {
	conn *TCPConnection
	addr string

	seq uint64

	pending sync.Map // map[uint64]*Future

	closed int32
}

func newTCPClient(addr string) (*TCPClient, error) {
	return newTCPClientContext(context.Background(), addr)
}

func newTCPClientContext(ctx context.Context, addr string) (*TCPClient, error) {
	dialer := net.Dialer{Timeout: 5 * time.Second}
	rawConn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	c := &TCPClient{
		conn: NewTCPConnection(rawConn),
		addr: addr,
	}

	go c.readLoop()
	return c, nil
}

func (c *TCPClient) nextSeq() uint64 {
	return atomic.AddUint64(&c.seq, 1)
}

func (c *TCPClient) SendAsync(msg *protocol.Message) (*Future, error) {
	return c.SendAsyncContext(context.Background(), msg)
}

func (c *TCPClient) SendAsyncContext(ctx context.Context, msg *protocol.Message) (*Future, error) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return nil, errors.New("connection closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	seq := c.nextSeq()
	msg.Header.RequestID = seq

	codecType := codec.Type(msg.Header.CodecType)
	if codecType == 0 {
		codecType = codec.JSON // Frames predating CodecType used JSON.
	}
	responseCodec, err := codec.New(codecType)
	if err != nil {
		return nil, err
	}
	future := NewFutureWithCodec(responseCodec)
	c.pending.Store(seq, future)

	wrote, err := c.conn.writeContext(ctx, msg)

	if err != nil {
		c.pending.Delete(seq)
		if wrote {
			c.fail(err)
		}
		return nil, err
	}

	go func() {
		select {
		case <-ctx.Done():
			if _, loaded := c.pending.LoadAndDelete(seq); loaded {
				future.Done(nil, ctx.Err())
			}
		case <-future.DoneChan():
		}
	}()

	return future, nil
}

func (c *TCPClient) readLoop() {
	for {
		msg, err := c.conn.Read()
		if err != nil {
			c.fail(err)
			return
		}

		seq := msg.Header.RequestID

		val, ok := c.pending.LoadAndDelete(seq)
		if !ok {
			continue
		}

		future := val.(*Future)

		if msg.Header.Error != "" {
			future.Done(nil, errors.New(msg.Header.Error))
		} else {
			future.Done(msg.Body, nil)
		}
	}
}

func (c *TCPClient) fail(err error) {
	_ = c.closeWithError(err)
}

func (c *TCPClient) closeWithError(err error) error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}

	// 关闭底层连接
	// log.Println("底层连接被关闭")
	closeErr := c.conn.Close()

	// 失败所有 pending
	c.pending.Range(func(key, value interface{}) bool {
		if pending, loaded := c.pending.LoadAndDelete(key); loaded {
			pending.(*Future).Done(nil, err)
		}
		return true
	})
	return closeErr
}

func (c *TCPClient) Close() error {
	return c.closeWithError(net.ErrClosed)
}
