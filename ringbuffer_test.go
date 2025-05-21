package anet

import (
	"sync"
	"testing"
)

func TestNextPow2Uint64(t *testing.T) {
	cases := []struct {
		input  uint64
		expect uint64
	}{
		{0, 1},
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{15, 16},
		{16, 16},
		{17, 32},
	}
	for _, c := range cases {
		out := nextPow2Uint64(c.input)
		if out != c.expect {
			t.Errorf("nextPow2Uint64(%d) = %d; want %d", c.input, out, c.expect)
		}
	}
}

func TestRingBufferBasic(t *testing.T) {
	// capacity size rounds up to next power of two
	rb := NewRingBuffer[int](3)
	cap := rb.Cap()
	if cap != 4 {
		t.Errorf("expected capacity 4, got %d", cap)
	}

	// empty state
	if l := rb.Len(); l != 0 {
		t.Errorf("expected length 0, got %d", l)
	}

	// enqueue up to capacity
	for i := range int(cap) {
		ok := rb.Enqueue(i)
		if !ok {
			t.Errorf("Enqueue(%d) failed, want succeed", i)
		}
		if l := rb.Len(); l != uint64(i+1) {
			t.Errorf("after enqueue %d, length = %d, want %d", i, l, i+1)
		}
	}
	// buffer is full; next enqueue should fail
	if ok := rb.Enqueue(100); ok {
		t.Errorf("Enqueue on full buffer succeeded, want fail")
	}

	// dequeue all items
	for i := range int(cap) {
		v, ok := rb.Dequeue()
		if !ok {
			t.Errorf("Dequeue failed at iteration %d, want succeed", i)
		}
		if v != i {
			t.Errorf("Dequeue returned %d, want %d", v, i)
		}
		if l := rb.Len(); l != cap-uint64(i)-1 {
			t.Errorf("after dequeue %d, length = %d, want %d", i, l, cap-uint64(i)-1)
		}
	}
	// now empty; next dequeue should fail
	_, ok := rb.Dequeue()
	if ok {
		t.Errorf("Dequeue on empty buffer succeeded, want fail")
	}
}

func TestRingBufferConcurrent(t *testing.T) {
	// simple concurrency test with multiple goroutines
	size := uint64(8)
	rb := NewRingBuffer[int](size)
	wg := sync.WaitGroup{}
	// producers
	for p := range 4 {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			for i := pid * 10; i < pid*10+10; i++ {
				for !rb.Enqueue(i) {
					// spin until space available
				}
			}
		}(p)
	}
	// consumers
	results := sync.Map{}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			count := 0
			for count < 10 {
				v, ok := rb.Dequeue()
				if !ok {
					continue
				}
				results.Store(v, true)
				count++
			}
		}()
	}
	wg.Wait()
	// verify all expected values present
	for v := range 40 {
		if _, ok := results.Load(v); !ok {
			t.Errorf("value %d missing from buffer results", v)
		}
	}
}
