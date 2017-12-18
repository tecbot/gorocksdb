package gorocksdb

import "C"

func NewMockSliceTransform() SliceTransform {
	return &mockSliceTransform{}
}

type mockSliceTransform struct {
	initiated bool
}

func (st *mockSliceTransform) Name() string                { return "gorocksdb.test" }
func (st *mockSliceTransform) CName() *C.char              { return C.CString("gorocksdb.test") }
func (st *mockSliceTransform) Transform(src []byte) []byte { return src[0:3] }
func (st *mockSliceTransform) InDomain(src []byte) bool    { return len(src) >= 3 }
func (st *mockSliceTransform) InRange(src []byte) bool     { return len(src) == 3 }
