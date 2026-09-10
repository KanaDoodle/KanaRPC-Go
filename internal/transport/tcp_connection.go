package transport

import (
	"bufio"
	"context"
	"github.com/KanaDoodle/KanaRPC-Go/internal/protocol"
	"io"
	"net"
	"sync"
	"time"
)

const BufferSize = 4096

// 包缓冲区（处理粘包）
type PacketBuffer struct {
	buf  []byte
	lock sync.Mutex
}

func (pb *PacketBuffer) Write(data []byte) {
	pb.lock.Lock()
	pb.buf = append(pb.buf, data...)
	pb.lock.Unlock()
}

func (pb *PacketBuffer) Read() ([]byte, error) {
	pb.lock.Lock()
	defer pb.lock.Unlock()

	// 最小包头长度校验
	if len(pb.buf) < protocol.FixedHeaderSize {
		return nil, nil
	}

	totalLen, err := protocol.DecodeFrameLength(pb.buf[:protocol.FixedHeaderSize])
	if err != nil {
		return nil, err
	}

	if len(pb.buf) < totalLen {
		return nil, nil
	}

	packet := make([]byte, totalLen)
	copy(packet, pb.buf[:totalLen])

	// 移动窗口
	pb.buf = pb.buf[totalLen:]
	return packet, nil
}

type TCPConnection struct {
	conn    net.Conn
	reader  *bufio.Reader
	buffer  *PacketBuffer
	readErr error // delivered only after all bytes accompanying the error are consumed

	writeMu chan struct{}
}

// 创建连接
func NewTCPConnection(conn net.Conn) *TCPConnection {
	return &TCPConnection{
		conn:    conn,
		reader:  bufio.NewReaderSize(conn, BufferSize),
		writeMu: make(chan struct{}, 1),
		buffer: &PacketBuffer{
			buf: make([]byte, 0, BufferSize*2),
		},
	}
}

func (tc *TCPConnection) Read() (*protocol.Message, error) {
	for {
		// 尝试从缓冲区取完整包
		packet, err := tc.buffer.Read()
		if err != nil {
			return nil, err
		}
		if packet != nil {
			return protocol.Decode(packet)
		}
		if tc.readErr != nil {
			return nil, tc.readErr
		}

		tmp := make([]byte, BufferSize)
		n, err := tc.reader.Read(tmp)
		if n > 0 {
			tc.buffer.Write(tmp[:n])
		}
		tc.readErr = err
	}
}

func (tc *TCPConnection) Write(msg *protocol.Message) error {
	_, err := tc.writeContext(context.Background(), msg)
	return err
}

// writeContext reports whether a socket write was attempted. A canceled waiter
// has not changed the stream; cancellation during a frame requires discarding
// the connection because the peer may have received only part of that frame.
func (tc *TCPConnection) writeContext(ctx context.Context, msg *protocol.Message) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	data, err := protocol.Encode(msg)
	if err != nil {
		return false, err
	}

	select {
	case tc.writeMu <- struct{}{}:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	defer func() { <-tc.writeMu }()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := tc.conn.SetWriteDeadline(deadline); err != nil {
			return false, err
		}
	}
	canceled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = tc.conn.SetWriteDeadline(time.Now())
		close(canceled)
	})
	defer func() {
		if !stop() {
			<-canceled
		}
		// Wait for the cancellation callback before clearing the shared deadline.
		_ = tc.conn.SetWriteDeadline(time.Time{})
	}()

	total := 0
	for total < len(data) {
		n, err := tc.conn.Write(data[total:])
		if err != nil {
			// A partial frame makes the stream unusable. Close before releasing
			// the write gate so the next queued writer cannot append to it.
			_ = tc.conn.Close()
			if ctx.Err() != nil {
				return true, ctx.Err()
			}
			if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
				return true, context.DeadlineExceeded
			}
			return true, err
		}
		if n == 0 {
			_ = tc.conn.Close()
			return true, io.ErrNoProgress
		}
		total += n
	}

	return true, nil
}

// 关闭连接
func (tc *TCPConnection) Close() error {
	if tcp, ok := tc.conn.(*net.TCPConn); ok {
		tcp.SetLinger(0)
	}
	return tc.conn.Close()
}

func (tc *TCPConnection) RemoteAddr() string {
	return tc.conn.RemoteAddr().String()
}
