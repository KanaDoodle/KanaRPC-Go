package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/KanaDoodle/KanaRPC-Go/internal/codec"
)

func TestMessageRoundTrip(t *testing.T) {
	wantBody := []byte(`{"message":"hello KanaRPC"}`)
	want := &Message{
		Header: &Header{
			RequestID:   42,
			ServiceName: "Greeter",
			MethodName:  "Hello",
			Compression: codec.CompressionGzip,
		},
		Body: wantBody,
	}

	encoded, err := Encode(want)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	got, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got.Header.RequestID != want.Header.RequestID {
		t.Fatalf("RequestID = %d, want %d", got.Header.RequestID, want.Header.RequestID)
	}
	if !bytes.Equal(got.Body, wantBody) {
		t.Fatalf("Body = %q, want %q", got.Body, wantBody)
	}
}

func TestDecodeFrameLengthRejectsOversizedBody(t *testing.T) {
	fixedHeader := make([]byte, FixedHeaderSize)
	binary.BigEndian.PutUint16(fixedHeader[0:2], Magic)
	binary.BigEndian.PutUint32(fixedHeader[6:10], MaxWireBodySize+1)

	if _, err := DecodeFrameLength(fixedHeader); err == nil {
		t.Fatal("DecodeFrameLength() expected oversized body error")
	}
}

func TestDecodeRejectsInvalidMagic(t *testing.T) {
	data := make([]byte, FixedHeaderSize)
	if _, err := Decode(data); err == nil {
		t.Fatal("Decode() expected invalid magic error")
	}
}

func FuzzDecodeNeverPanics(f *testing.F) {
	valid, err := Encode(&Message{
		Header: &Header{RequestID: 1, Compression: codec.CompressionNone},
		Body:   []byte("seed"),
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte{0x12, 0x34})
	f.Add(make([]byte, FixedHeaderSize))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = Decode(data)
	})
}
