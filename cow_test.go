package gorocksdb

import (
	"sync"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestCOWList(t *testing.T) {
	cl := NewCOWList()
	cl.Append("hello")
	cl.Append("world")
	cl.Append("!")
	ensure.DeepEqual(t, cl.Get(0), "hello")
	ensure.DeepEqual(t, cl.Get(1), "world")
	ensure.DeepEqual(t, cl.Get(2), "!")
}

func TestCOWListMT(t *testing.T) {
	cl := NewCOWList()
	expectedRes := make([]int, 3)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			index := cl.Append(v)
			expectedRes[index] = v
		}(i)
	}
	wg.Wait()
	for i, v := range expectedRes {
		ensure.DeepEqual(t, cl.Get(i), v)
	}
}
