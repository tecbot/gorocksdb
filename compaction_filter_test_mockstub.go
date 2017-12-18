package gorocksdb

import "C"

func NewMockCompactionFilter(f func(level int, key, val []byte) (remove bool, newVal []byte)) CompactionFilter {
	return &mockCompactionFilter{
		filter: f,
	}
}

type mockCompactionFilter struct {
	filter func(level int, key, val []byte) (remove bool, newVal []byte)
}

func (m *mockCompactionFilter) Name() string   { return "gorocksdb.test" }
func (m *mockCompactionFilter) CName() *C.char { return C.CString("gorocksdb.test") }
func (m *mockCompactionFilter) Filter(level int, key, val []byte) (bool, []byte) {
	return m.filter(level, key, val)
}

func GetCName(c *C.char) string { return C.GoString(c) }
