package loadbalance

import "github.com/KanaDoodle/KanaRPC-Go/internal/registry"

type LoadBalancer interface {
	Select([]registry.Instance) registry.Instance
}
