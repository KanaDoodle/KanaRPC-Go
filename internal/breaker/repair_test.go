package breaker

import (
	"context"
	"testing"
	"time"
)

func TestRepairCancellationNeutral(t *testing.T) {
	cb := NewCircuitBreaker(10, .6, time.Millisecond)
	for i := 0; i < 20; i++ {
		finish, ok := cb.Acquire()
		if !ok {
			t.Fatal("local cancellations opened circuit")
		}
		finish(context.Canceled)
	}
	if cb.failureCount != 0 || cb.successCount != 0 {
		t.Fatal("cancellation counted as backend result")
	}
	for i := 0; i < 10; i++ {
		cb.RecordFailure()
	}
	time.Sleep(3 * time.Millisecond)
	finish, ok := cb.Acquire()
	if !ok {
		t.Fatal("half open denied")
	}
	finish(context.Canceled)
	finish, ok = cb.Acquire()
	if !ok {
		t.Fatal("canceled probe leaked half-open slot")
	}
	finish(nil)
	if cb.State() != Closed {
		t.Fatal("half-open recovery failed")
	}
}
func TestRepairDeadlineRemainsFailure(t *testing.T) {
	cb := NewCircuitBreaker(2, .5, time.Second)
	for i := 0; i < 2; i++ {
		finish, _ := cb.Acquire()
		finish(context.DeadlineExceeded)
	}
	if cb.State() != Open {
		t.Fatal("backend timeouts disabled breaker")
	}
}
