package gorocksdb

import "bytes"
import "C"

func NewMockBytesReverseComparator() Comparator {
	return &bytesReverseComparator{}
}

type bytesReverseComparator struct{}

func (cmp *bytesReverseComparator) Name() string   { return "gorocksdb.bytes-reverse" }
func (cmp *bytesReverseComparator) CName() *C.char { return C.CString("gorocksdb.bytes-reverse") }
func (cmp *bytesReverseComparator) Compare(a, b []byte) int {
	return bytes.Compare(a, b) * -1
}
