package client

import (
	"errors"
	"github.com/KanaDoodle/KanaRPC-Go/internal/loadbalance"
	"github.com/KanaDoodle/KanaRPC-Go/internal/registry"
	"sync"
	"testing"
)

func TestReleaseServiceStateAndClose(t *testing.T) {
	for _, strategy := range []loadbalance.LoadBalancer{&loadbalance.RoundRobin{}, loadbalance.NewRandom(), loadbalance.NewWeightedRR([]int{1, 1})} {
		c, err := NewClient(nil, WithClientLoadBalancer(strategy))
		if err != nil {
			t.Fatal(err)
		}
		a, err := c.serviceBalancer("S")
		if err != nil {
			t.Fatal(err)
		}
		b, err := c.serviceBalancer("T")
		if err != nil {
			t.Fatal(err)
		}
		if a == b {
			t.Fatal("shared service state")
		}
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					lb, e := c.serviceBalancer("S")
					if e == nil {
						if lb.Select([]registry.Instance{{Addr: "a"}, {Addr: "b"}}).Addr == "" {
							t.Error("strategy returned no member")
						}
					} else if !errors.Is(e, ErrClientClosed) {
						t.Error(e)
					}
				}
			}()
		}
		c.Close()
		wg.Wait()
		if len(c.balancers) != 0 {
			t.Fatal("Close retained service state")
		}
		if _, err = c.serviceBalancer("new"); !errors.Is(err, ErrClientClosed) {
			t.Fatal("Close allowed map regrowth")
		}
	}
}
