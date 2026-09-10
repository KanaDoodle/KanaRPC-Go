package loadbalance

import (
	"testing"

	"github.com/KanaDoodle/KanaRPC-Go/internal/registry"
)

func TestRoundRobinStartsFromFirstAndHandlesEmptyList(t *testing.T) {
	balancer := NewRR()
	instances := []registry.Instance{{Addr: "a"}, {Addr: "b"}}
	if got := balancer.Select(instances).Addr; got != "a" {
		t.Fatalf("first Select() = %q, want a", got)
	}
	if got := balancer.Select(instances).Addr; got != "b" {
		t.Fatalf("second Select() = %q, want b", got)
	}
	if got := balancer.Select(nil); got != (registry.Instance{}) {
		t.Fatalf("empty Select() = %#v", got)
	}
}

func TestWeightedRoundRobinDistribution(t *testing.T) {
	balancer := NewWeightedRR([]int{5, 1})
	instances := []registry.Instance{{Addr: "heavy"}, {Addr: "light"}}
	counts := map[string]int{}
	for i := 0; i < 6; i++ {
		counts[balancer.Select(instances).Addr]++
	}
	if counts["heavy"] != 5 || counts["light"] != 1 {
		t.Fatalf("distribution = %#v, want heavy=5 light=1", counts)
	}
}
