package breaker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCircuitBreakerTransitionsAndAllowsSingleProbe(t *testing.T) {
	breaker := NewCircuitBreaker(4, 0.5, 10*time.Millisecond)
	breaker.RecordFailure()
	breaker.RecordFailure()
	breaker.RecordSuccess()
	breaker.RecordSuccess()
	if state := breaker.State(); state != Open {
		t.Fatalf("state = %v, want Open", state)
	}
	if breaker.Allow() {
		t.Fatal("Allow() = true while breaker is open")
	}

	time.Sleep(15 * time.Millisecond)
	var allowed int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if breaker.Allow() {
				atomic.AddInt32(&allowed, 1)
			}
		}()
	}
	wg.Wait()
	if allowed != 1 {
		t.Fatalf("half-open probes = %d, want 1", allowed)
	}

	breaker.RecordSuccess()
	if state := breaker.State(); state != Closed {
		t.Fatalf("state after successful probe = %v, want Closed", state)
	}
}

func TestCircuitBreakerResetsHealthyFixedWindow(t *testing.T) {
	breaker := NewCircuitBreaker(3, 0.5, time.Second)
	breaker.RecordSuccess()
	breaker.RecordSuccess()
	breaker.RecordSuccess()
	breaker.RecordFailure()
	breaker.RecordFailure()
	if state := breaker.State(); state != Closed {
		t.Fatalf("state = %v, want Closed before the second window is full", state)
	}
}
