package limiter

import (
	"bytes"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func tokenRefillGoroutines() int {
	var stacks bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&stacks, 2)
	return bytes.Count(stacks.Bytes(), []byte("limiter.NewTokenBucket.func1("))
}

func TestTokenBucketDoesNotLeaveBackgroundGoroutines(t *testing.T) {
	before := tokenRefillGoroutines()
	for i := 0; i < 20; i++ {
		bucket := NewTokenBucket(1)
		bucket.Allow()
	}
	time.Sleep(10 * time.Millisecond)
	if leaked := tokenRefillGoroutines() - before; leaked != 0 {
		t.Fatalf("discarded token buckets left %d permanent refill goroutines", leaked)
	}
}

func TestTokenBucketRefillsWithoutAccumulatingIdleWindows(t *testing.T) {
	bucket := NewTokenBucket(3)
	for i := 0; i < 3; i++ {
		if !bucket.Allow() {
			t.Fatal("initial quota unavailable")
		}
	}
	if bucket.Allow() {
		t.Fatal("quota exceeded")
	}
	bucket.mu.Lock()
	bucket.lastRefill = time.Now().Add(-3 * time.Second)
	bucket.mu.Unlock()
	var wg sync.WaitGroup
	var allowed atomic.Int32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if bucket.Allow() {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != 3 {
		t.Fatalf("allowed %d calls after idle windows; want 3", got)
	}
}
