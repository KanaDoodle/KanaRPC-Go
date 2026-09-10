package loadbalance

import (
	"github.com/KanaDoodle/KanaRPC-Go/internal/registry"
	"log"
	"sync"
)

// 平滑权重轮询算法
type WeightedRR struct {
	mu            sync.Mutex
	weights       []int // 固定权重
	currentWeight []int // 当前权重（动态变化）
	totalWeight   int   // 权重总和
}

// 初始化
func NewWeightedRR(weights []int) *WeightedRR {
	w := &WeightedRR{
		weights:       make([]int, len(weights)),
		currentWeight: make([]int, len(weights)),
	}

	total := 0
	for i, wt := range weights {
		if wt < 0 {
			wt = 0
		}
		w.weights[i] = wt
		total += wt
	}
	w.totalWeight = total

	return w
}

func (w *WeightedRR) Select(list []registry.Instance) registry.Instance {
	if len(list) == 0 {
		return registry.Instance{}
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// 如果实例数量变化，说明需要重置(这边直接返回一个空了)
	if len(list) != len(w.weights) {
		log.Println("实例数量和权重列表数量不一致")
		return registry.Instance{}
	}

	maxIdx := -1
	// 所有节点 currentWeight += weight
	for i := 0; i < len(list); i++ {
		w.currentWeight[i] += w.weights[i]

		if maxIdx == -1 || w.currentWeight[i] > w.currentWeight[maxIdx] {
			maxIdx = i
		}
	}

	// 选中最大权重节点
	w.currentWeight[maxIdx] -= w.totalWeight
	return list[maxIdx]
}

func (w *WeightedRR) NewBalancer() LoadBalancer {
	w.mu.Lock()
	defer w.mu.Unlock()
	return NewWeightedRR(w.weights)
}
