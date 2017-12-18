package gorocksdb

import "C"

func NewMockFilterPolicy(cf func(keys [][]byte) []byte, kmm func(key, filter []byte) bool) FilterPolicy {
	return &mockFilterPolicy{cf, kmm}
}

type mockFilterPolicy struct {
	createFilter func(keys [][]byte) []byte
	keyMayMatch  func(key, filter []byte) bool
}

func (m *mockFilterPolicy) Name() string   { return "gorocksdb.test" }
func (m *mockFilterPolicy) CName() *C.char { return C.CString("gorocksdb.test") }
func (m *mockFilterPolicy) CreateFilter(keys [][]byte) []byte {
	return m.createFilter(keys)
}
func (m *mockFilterPolicy) KeyMayMatch(key, filter []byte) bool {
	return m.keyMayMatch(key, filter)
}
