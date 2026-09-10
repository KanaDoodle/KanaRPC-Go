package breaker

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

type CircuitBreaker struct {
	mu sync.Mutex

	state State

	// 统计数据
	failureCount int
	successCount int

	// 配置参数
	windowSize       int           // 统计窗口大小（次数）
	failureThreshold float64       // 失败率阈值
	openTimeout      time.Duration // 熔断持续时间

	// 状态控制
	lastStateChange time.Time
	halfOpenProbe   bool // 半开状态下是否已有探测请求
	generation      uint64
}

func NewCircuitBreaker(windowSize int, failureThreshold float64, openTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:            Closed,
		windowSize:       windowSize,
		failureThreshold: failureThreshold,
		openTimeout:      openTimeout,
		lastStateChange:  time.Now(),
	}
}

// Acquire admits one request and returns an idempotent completion function.
// Results from requests admitted before a state change cannot complete a newer
// half-open probe or affect the new state's failure window.
func (cb *CircuitBreaker) Acquire() (finish func(error), allowed bool) {
	cb.mu.Lock()
	allowed = cb.allowLocked()
	generation := cb.generation
	cb.mu.Unlock()
	if !allowed {
		return nil, false
	}
	var once sync.Once
	return func(err error) {
		once.Do(func() {
			cb.mu.Lock()
			defer cb.mu.Unlock()
			if generation != cb.generation {
				return
			}
			if errors.Is(err, context.Canceled) {
				// Local cancellation is neutral. Release a half-open slot without claiming
				// success or recording a backend failure.
				if cb.state == HalfOpen {
					cb.halfOpenProbe = false
				}
				return
			}
			if err == nil {
				cb.recordSuccessLocked()
			} else {
				cb.recordFailureLocked()
			}
		})
	}, true
}

// Allow is retained for callers that record outcomes synchronously. Concurrent
// request lifecycles must use Acquire to associate results with their admission.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.allowLocked()
}

func (cb *CircuitBreaker) allowLocked() bool {
	switch cb.state {

	case Closed:
		return true

	case Open:
		// 熔断时间到了，进入半开
		if time.Since(cb.lastStateChange) > cb.openTimeout {
			cb.state = HalfOpen
			cb.generation++
			// The current request is the single half-open probe.
			cb.halfOpenProbe = true
			return true
		}
		return false

	case HalfOpen:
		// 只允许一个探测请求
		if cb.halfOpenProbe {
			return false
		}
		cb.halfOpenProbe = true
		return true
	}

	return true
}
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.recordSuccessLocked()
}

func (cb *CircuitBreaker) recordSuccessLocked() {
	switch cb.state {

	case Closed:
		cb.successCount++
		cb.evaluateWindow()

	case HalfOpen:
		// 探测成功 → 恢复
		cb.toClosed()

	case Open:
		//按道理是不会进入这块的
		log.Println("理论不发生触发")
	}
}
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.recordFailureLocked()
}

func (cb *CircuitBreaker) recordFailureLocked() {
	switch cb.state {

	case Closed:
		cb.failureCount++
		cb.evaluateWindow()

	case HalfOpen:
		// 探测失败 → 重新熔断
		cb.toOpen()

	case Open:
		// 已经熔断，不处理
	}
}
func (cb *CircuitBreaker) toOpen() {
	cb.state = Open
	cb.generation++
	cb.lastStateChange = time.Now()
	cb.resetCounts()
	cb.halfOpenProbe = false
}

func (cb *CircuitBreaker) toClosed() {
	cb.state = Closed
	cb.generation++
	cb.lastStateChange = time.Now()
	cb.resetCounts()
	cb.halfOpenProbe = false
}
func (cb *CircuitBreaker) resetCounts() {
	cb.failureCount = 0
	cb.successCount = 0
}

func (cb *CircuitBreaker) evaluateWindow() {
	total := cb.failureCount + cb.successCount
	if total < cb.windowSize {
		return
	}
	if float64(cb.failureCount)/float64(total) >= cb.failureThreshold {
		cb.toOpen()
		return
	}
	cb.resetCounts()
}

func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// Eligible is a non-reserving snapshot for load balancing. Acquire still gates
// the selected instance, including racing half-open probes.
func (cb *CircuitBreaker) Eligible() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state == Closed || (cb.state == Open && time.Since(cb.lastStateChange) > cb.openTimeout) || (cb.state == HalfOpen && !cb.halfOpenProbe)
}
