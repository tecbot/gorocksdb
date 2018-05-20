package stack_test

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/facebookgo/stack"
)

func BenchmarkCallersMulti(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stack.CallersMulti(0)
	}
}

func BenchmarkCallers(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stack.Callers(0)
	}
}

func BenchmarkCaller(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stack.Caller(0)
	}
}

func BenchmarkRuntimeCallersReusePCS(b *testing.B) {
	pcs := make([]uintptr, 32)
	for i := 0; i < b.N; i++ {
		runtime.Callers(0, pcs)
	}
}

func BenchmarkRuntimeCallersMakePCS(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pcs := make([]uintptr, 32)
		runtime.Callers(0, pcs)
	}
}

func BenchmarkRuntimeCallersFixedPCS(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var pcs [32]uintptr
		runtime.Callers(0, pcs[:])
	}
}

func BenchmarkRuntimeCallersSyncPool(b *testing.B) {
	pool := sync.Pool{New: func() interface{} { return make([]uintptr, 32) }}
	for i := 0; i < b.N; i++ {
		pcs := pool.Get().([]uintptr)
		runtime.Callers(0, pcs[:])
		pcs = pcs[0:]
		pool.Put(pcs)
	}
}

func BenchmarkSprintf(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fmt.Sprintf("hello")
	}
}
