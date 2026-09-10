package protocol

import (
	"bytes"
	"encoding/binary"
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
