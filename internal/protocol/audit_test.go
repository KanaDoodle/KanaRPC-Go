package protocol

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/KanaDoodle/KanaRPC-Go/internal/codec"
)

func TestAuditDecodeRejectsDecompressionBomb(t *testing.T) {
	compressed, err := codec.Compress(bytes.Repeat([]byte("a"), MaxBodySize+1), codec.CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	// Construct a peer frame independently of Encode's own size policy.
	header := []byte(`{"RequestID":1,"Compression":1}`)
	frame := make([]byte, FixedHeaderSize+len(header)+len(compressed))
	binary.BigEndian.PutUint16(frame, Magic)
	binary.BigEndian.PutUint32(frame[2:], uint32(len(header)))
	binary.BigEndian.PutUint32(frame[6:], uint32(len(compressed)))
	copy(frame[FixedHeaderSize:], header)
	copy(frame[FixedHeaderSize+len(header):], compressed)
	msg, err := Decode(frame)
	if err == nil {
		t.Fatalf("accepted %d compressed bytes expanding to %d bytes (limit %d)", len(compressed), len(msg.Body), MaxBodySize)
	}
}

func TestAuditEncodeRejectsOversizedUncompressedBody(t *testing.T) {
	_, err := Encode(&Message{Header: &Header{Compression: codec.CompressionGzip}, Body: make([]byte, MaxBodySize+1)})
	if err == nil {
		t.Fatal("Encode accepted a body above the decompressed size limit")
	}
}

func TestAuditEncodeNilMessage(t *testing.T) {
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("Encode(nil) panicked: %v", p)
		}
	}()
	if _, err := Encode(nil); err == nil {
		t.Fatal("Encode(nil) must return an error")
	}
}

func TestAuditCompressedBodyAtLimit(t *testing.T) {
	frame, err := Encode(&Message{Header: &Header{Compression: codec.CompressionGzip}, Body: make([]byte, MaxBodySize)})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := Decode(frame)
	if err != nil || len(msg.Body) != MaxBodySize {
		t.Fatalf("body at limit rejected: %v", err)
	}
}

// gzip stores incompressible input instead of shrinking it, so a body that is
// exactly MaxBodySize decompressed is slightly larger on the wire. Encode must
// still accept it, and Decode must round-trip it.
func TestAuditIncompressibleBodyAtLimitRoundTrips(t *testing.T) {
	body := make([]byte, MaxBodySize)
	// Pseudo-random bytes are effectively incompressible; the all-zero fixture
	// in TestAuditCompressedBodyAtLimit does not cover this case.
	if _, err := rand.New(rand.NewSource(1)).Read(body); err != nil {
		t.Fatal(err)
	}
	header := &Header{RequestID: 7, Compression: codec.CompressionGzip}

	frame, err := Encode(&Message{Header: header, Body: body})
	if err != nil {
		t.Fatalf("Encode rejected an incompressible body at the decompressed limit: %v", err)
	}
	msg, err := Decode(frame)
	if err != nil {
		t.Fatalf("Decode rejected a frame produced by Encode: %v", err)
	}
	if len(msg.Body) != MaxBodySize || !bytes.Equal(msg.Body, body) {
		t.Fatalf("round trip changed the body: got %d bytes", len(msg.Body))
	}
}

// The wire bound must stay finite even though it is larger than the
// decompressed bound, and it must be the bound DecodeFrameLength enforces.
func TestAuditWireBodyBoundIsFinite(t *testing.T) {
	if MaxWireBodySize <= MaxBodySize {
		t.Fatalf("MaxWireBodySize (%d) must exceed MaxBodySize (%d) to leave room for gzip overhead",
			MaxWireBodySize, MaxBodySize)
	}
	fixedHeader := make([]byte, FixedHeaderSize)
	binary.BigEndian.PutUint16(fixedHeader[0:2], Magic)
	binary.BigEndian.PutUint32(fixedHeader[6:10], uint32(MaxWireBodySize+1))
	if _, err := DecodeFrameLength(fixedHeader); err == nil {
		t.Fatal("DecodeFrameLength accepted a wire body above MaxWireBodySize")
	}
}

func FuzzMessageRoundTrip(f *testing.F) {
	f.Add([]byte("rpc"), uint64(42), false)
	f.Add([]byte(`{"a":1}`), uint64(0), true)
	f.Add([]byte{}, ^uint64(0), true)
	f.Fuzz(func(t *testing.T, body []byte, requestID uint64, gzip bool) {
		if len(body) > 64<<10 {
			t.Skip()
		}
		header := &Header{RequestID: requestID, CodecType: CodecTypeJSON}
		if gzip {
			header.Compression = codec.CompressionGzip
		}
		frame, err := Encode(&Message{Header: header, Body: body})
		if err != nil {
			t.Fatal(err)
		}
		msg, err := Decode(frame)
		if err != nil {
			t.Fatal(err)
		}
		if *msg.Header != *header || !bytes.Equal(msg.Body, body) {
			t.Fatal("round trip changed header or body")
		}
	})
}

func BenchmarkMessageRoundTrip(b *testing.B) {
	for _, compression := range []codec.CompressionType{codec.CompressionNone, codec.CompressionGzip} {
		name := "plain"
		if compression == codec.CompressionGzip {
			name = "gzip"
		}
		b.Run(name, func(b *testing.B) {
			msg := &Message{Header: &Header{RequestID: 1, Compression: compression}, Body: bytes.Repeat([]byte("payload "), 128)}
			b.ReportAllocs()
			b.SetBytes(int64(len(msg.Body)))
			for i := 0; i < b.N; i++ {
				frame, err := Encode(msg)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := Decode(frame); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
