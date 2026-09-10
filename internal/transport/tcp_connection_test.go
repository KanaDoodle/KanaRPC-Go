package transport

import (
	"testing"

	"github.com/KanaDoodle/KanaRPC-Go/internal/codec"
	"github.com/KanaDoodle/KanaRPC-Go/internal/protocol"
)

func TestPacketBufferHandlesPartialAndStickyPackets(t *testing.T) {
	first, err := protocol.Encode(&protocol.Message{
		Header: &protocol.Header{RequestID: 1, Compression: codec.CompressionNone},
		Body:   []byte("first"),
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := protocol.Encode(&protocol.Message{
		Header: &protocol.Header{RequestID: 2, Compression: codec.CompressionNone},
		Body:   []byte("second"),
	})
	if err != nil {
		t.Fatal(err)
	}

	pb := &PacketBuffer{}
	pb.Write(first[:5])
	packet, err := pb.Read()
	if err != nil {
		t.Fatal(err)
	}
	if packet != nil {
		t.Fatal("Read() returned a packet before the fixed header was complete")
	}

	pb.Write(append(first[5:], second...))
	packet, err = pb.Read()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := protocol.Decode(packet)
	if err != nil || msg.Header.RequestID != 1 {
		t.Fatalf("first packet = %#v, err = %v", msg, err)
	}

	packet, err = pb.Read()
	if err != nil {
		t.Fatal(err)
	}
	msg, err = protocol.Decode(packet)
	if err != nil || msg.Header.RequestID != 2 {
		t.Fatalf("second packet = %#v, err = %v", msg, err)
	}
}
