package server

import (
	"context"
	"fmt"
	"github.com/KanaDoodle/KanaRPC-Go/internal/codec"
	"github.com/KanaDoodle/KanaRPC-Go/internal/protocol"
	"github.com/KanaDoodle/KanaRPC-Go/internal/transport"
	"log"
	"reflect"
)

type Handler struct {
	codec codec.Codec
}

func NewHandler(s interface{}, opts ...HandleOption) (*Handler, error) {
	h := &Handler{}

	for _, opt := range opts {
		if err := opt(h); err != nil {
			return nil, err
		}
	}

	if h.codec == nil {
		return nil, fmt.Errorf("codec must not be nil")
	}

	return h, nil
}

func (h *Handler) Process(conn *transport.TCPConnection, msg *protocol.Message, server interface{}) {
	defer func() {
		if recovered := recover(); recovered != nil {
			h.writeError(conn, msg.Header.RequestID, fmt.Sprintf("handler panic: %v", recovered))
		}
	}()

	result, err := h.invoke(
		context.Background(),
		server,
		msg.Header.ServiceName,
		msg.Header.MethodName,
		msg.Body,
	)

	if err != nil {
		h.writeError(conn, msg.Header.RequestID, err.Error())
		return
	}

	var body []byte
	if result != nil {
		var marshalErr error
		body, marshalErr = h.codec.Marshal(result)
		if marshalErr != nil {
			log.Println("marshal error:", marshalErr)
			h.writeError(conn, msg.Header.RequestID, marshalErr.Error())
			return
		}
	}

	resp := &protocol.Message{
		Header: &protocol.Header{
			RequestID:   msg.Header.RequestID,
			Compression: codec.CompressionGzip,
		},
		Body: body,
	}

	conn.Write(resp)
}

func (h *Handler) writeError(conn *transport.TCPConnection, requestID uint64, errMsg string) {
	resp := &protocol.Message{
		Header: &protocol.Header{
			RequestID:   requestID,
			Error:       errMsg,
			Compression: codec.CompressionGzip,
		},
	}
	conn.Write(resp)
}

func (h *Handler) invoke(ctx context.Context, service interface{}, serviceName, methodName string, body []byte) (interface{}, error) {
	if service == nil {
		return nil, fmt.Errorf("service not found: %s", serviceName)
	}

	serviceValue := reflect.ValueOf(service)
	method := serviceValue.MethodByName(methodName)
	if !method.IsValid() {
		return nil, fmt.Errorf("method not found: %s.%s", serviceName, methodName)
	}

	methodType := method.Type()
	numIn := methodType.NumIn()
	numOut := methodType.NumOut()

	args := make([]reflect.Value, 0, numIn)

	// net/rpc 风格
	// func(req *Req, reply *Resp) error

	if numIn == 2 &&
		methodType.In(0).Kind() == reflect.Ptr &&
		methodType.In(1).Kind() == reflect.Ptr &&
		numOut == 1 &&
		methodType.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) {

		// 构造 req
		reqType := methodType.In(0)
		req := reflect.New(reqType.Elem())

		if len(body) > 0 {
			if err := h.codec.Unmarshal(body, req.Interface()); err != nil {
				return nil, err
			}
		}

		// 构造 reply
		replyType := methodType.In(1)
		reply := reflect.New(replyType.Elem())

		args = append(args, req)
		args = append(args, reply)

		results := method.Call(args)

		// 处理 error
		if errVal := results[0].Interface(); errVal != nil {
			return nil, errVal.(error)
		}

		return reply.Interface(), nil
	}
	return nil, fmt.Errorf("unsupported method signature: %s.%s", serviceName, methodName)
}
