package breaker

import (
	"errors"
	"testing"
	"time"
)

func admitAuditRequest(cb *CircuitBreaker) (func(error), bool) {
	return cb.Acquire()
}

func TestCircuitBreakerIgnoresResultsFromPreviousGeneration(t *testing.T) {
	cb := NewCircuitBreaker(1, 1, time.Second)
	lateSuccess, ok := admitAuditRequest(cb)
	if !ok {
		t.Fatal("first request rejected")
	}
	failure, ok := admitAuditRequest(cb)
	if !ok {
		t.Fatal("second request rejected")
	}
	failure(errors.New("unavailable"))
	cb.mu.Lock()
	cb.lastStateChange = time.Now().Add(-2 * time.Second)
	cb.mu.Unlock()
	probe, ok := admitAuditRequest(cb)
	if !ok {
		t.Fatal("half-open probe rejected")
	}
	lateSuccess(nil)
	if got := cb.State(); got != HalfOpen {
		t.Fatalf("old request success changed active probe state to %v; want HalfOpen", got)
	}
	probe(errors.New("still unavailable"))
	if got := cb.State(); got != Open {
		t.Fatalf("probe failure left state %v; want Open", got)
	}
}

func TestCircuitBreakerRequestCompletionIsIdempotent(t *testing.T) {
	cb := NewCircuitBreaker(2, 1, time.Second)
	finish, ok := cb.Acquire()
	if !ok {
		t.Fatal("request rejected")
	}
	finish(errors.New("failure"))
	finish(errors.New("failure"))
	if got := cb.State(); got != Closed {
		t.Fatalf("duplicate completion changed state to %v", got)
	}
	second, ok := cb.Acquire()
	if !ok {
		t.Fatal("second request rejected")
	}
	second(errors.New("failure"))
	if got := cb.State(); got != Open {
		t.Fatalf("two failed requests left state %v; want Open", got)
	}
}
