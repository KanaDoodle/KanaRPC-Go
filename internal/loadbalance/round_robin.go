package loadbalance

import (
	"github.com/KanaDoodle/KanaRPC-Go/internal/registry"
	"sync/atomic"
)

type RoundRobin struct {
	idx uint64
}

func NewRR() *RoundRobin {
	r := &RoundRobin{}
	return r
}

func (r *RoundRobin) Select(list []registry.Instance) registry.Instance {
	if len(list) == 0 {
		return registry.Instance{}
	}
	i := atomic.AddUint64(&r.idx, 1) - 1
	return list[i%uint64(len(list))]
}

func (*RoundRobin) NewBalancer() LoadBalancer { return &RoundRobin{} }
