package limiter

import (
	"sync"
	"time"
)

type TokenBucket struct {
	tokens     int
	rate       int
	lastRefill time.Time
	mu         sync.Mutex
}

func NewTokenBucket(rate int) *TokenBucket {
	return &TokenBucket{tokens: rate, rate: rate, lastRefill: time.Now()}
}

func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	// Keep the existing per-second quota semantics without a lifetime-long
	// refill goroutine. Advance by whole windows so idle periods do not drift
	// the original window boundaries or accumulate extra quota.
	if elapsed := time.Since(tb.lastRefill); elapsed >= time.Second {
		tb.tokens = tb.rate
		tb.lastRefill = tb.lastRefill.Add(elapsed / time.Second * time.Second)
	}
	if tb.tokens > 0 {
		tb.tokens--
		return true
	}
	return false
}
