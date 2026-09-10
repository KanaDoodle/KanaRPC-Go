package transport

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestFutureCompletesOnlyOnce(t *testing.T) {
	future := NewFuture()
	var callbackCount int32
	future.OnComplete(func(error) {
		atomic.AddInt32(&callbackCount, 1)
	})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			future.Done([]byte("ok"), nil)
		}()
	}
	wg.Wait()

	result, err := future.Wait()
	if err != nil || string(result) != "ok" {
		t.Fatalf("Wait() = %q, %v", result, err)
	}
	if got := atomic.LoadInt32(&callbackCount); got != 1 {
		t.Fatalf("callback count = %d, want 1", got)
	}
}

func TestFutureLateCallbackReceivesResult(t *testing.T) {
	wantErr := errors.New("failed")
	future := NewFuture()
	future.Done(nil, wantErr)

	called := make(chan error, 1)
	future.OnComplete(func(err error) { called <- err })
	if got := <-called; !errors.Is(got, wantErr) {
		t.Fatalf("callback error = %v, want %v", got, wantErr)
	}
}
