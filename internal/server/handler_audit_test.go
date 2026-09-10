package server

import (
	"context"
	"testing"

	"github.com/KanaDoodle/KanaRPC-Go/internal/codec"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type auditProtoService struct{}

func (*auditProtoService) Echo(req *wrapperspb.Int64Value, reply *wrapperspb.Int64Value) error {
	reply.Value = req.Value
	return nil
}

func TestHandlerPreservesProtobufReplyPointer(t *testing.T) {
	h, err := NewHandler(nil, WithHandlerCodec(codec.PROTO))
	if err != nil {
		t.Fatal(err)
	}
	body, err := proto.Marshal(wrapperspb.Int64(42))
	if err != nil {
		t.Fatal(err)
	}
	result, err := h.invoke(context.Background(), &auditProtoService{}, "Proto", "Echo", body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.codec.Marshal(result); err != nil {
		t.Fatalf("handler returned an unencodable protobuf reply: %v", err)
	}
}
