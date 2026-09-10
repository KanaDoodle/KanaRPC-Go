package transport

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC-Go/internal/protocol"
)

// The io.Reader contract permits returning final data together with EOF.
type finalFrameConn struct{ data []byte }

func (c *finalFrameConn) Read(p []byte) (int, error) {
	n := copy(p, c.data)
	c.data = c.data[n:]
	return n, io.EOF
}
func (*finalFrameConn) Write(p []byte) (int, error)      { return len(p), nil }
func (*finalFrameConn) Close() error                     { return nil }
func (*finalFrameConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*finalFrameConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*finalFrameConn) SetDeadline(time.Time) error      { return nil }
func (*finalFrameConn) SetReadDeadline(time.Time) error  { return nil }
func (*finalFrameConn) SetWriteDeadline(time.Time) error { return nil }

func TestAuditReadPreservesFinalFrameWithEOF(t *testing.T) {
	frame, err := protocol.Encode(&protocol.Message{Header: &protocol.Header{RequestID: 42}, Body: []byte("final")})
	if err != nil {
		t.Fatal(err)
	}
	conn := NewTCPConnection(&finalFrameConn{data: append(append([]byte{}, frame...), frame...)})
	for i := 0; i < 2; i++ {
		msg, err := conn.Read()
		if err != nil {
			t.Fatalf("final frame %d lost: %v", i, err)
		}
		if msg.Header.RequestID != 42 || string(msg.Body) != "final" {
			t.Fatalf("unexpected final frame: %+v", msg)
		}
	}
	if _, err := conn.Read(); err != io.EOF {
		t.Fatalf("after final frames: got %v, want EOF", err)
	}
}
