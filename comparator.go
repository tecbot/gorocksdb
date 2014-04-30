package gorocksdb

// #include "rocksdb/c.h"
// #include "gorocksdb.h"
import "C"
import (
	"unsafe"
)

// A Comparator object provides a total order across slices that are
// used as keys in an sstable or a database.
type Comparator struct {
	c *C.rocksdb_comparator_t
}

type ComparatorHandler interface {
	// Three-way comparison. Returns value:
	//   < 0 iff "a" < "b",
	//   == 0 iff "a" == "b",
	//   > 0 iff "a" > "b"
	Compare(a, b []byte) int

	// The name of the comparator.
	Name() string
}

// NewComparator creates a new comparator for the given handler.
func NewComparator(handler ComparatorHandler) *Comparator {
	h := unsafe.Pointer(&handler)
	return NewNativeComparator(C.gorocksdb_comparator_create(h))
}

// NewNativeComparator allocates a Comparator object.
func NewNativeComparator(c *C.rocksdb_comparator_t) *Comparator {
	return &Comparator{c}
}

// Destroy deallocates the Comparator object.
func (self *Comparator) Destroy() {
	C.rocksdb_comparator_destroy(self.c)
	self.c = nil
}

//export gorocksdb_comparator_compare
func gorocksdb_comparator_compare(handler *ComparatorHandler, cKeyA *C.char, cKeyALen C.size_t, cKeyB *C.char, cKeyBLen C.size_t) C.int {
	keyA := CharToByte(cKeyA, cKeyALen)
	keyB := CharToByte(cKeyB, cKeyBLen)

	compare := (*handler).Compare(keyA, keyB)

	return C.int(compare)
}

//export gorocksdb_comparator_name
func gorocksdb_comparator_name(handler *ComparatorHandler) *C.char {
	return StringToChar((*handler).Name())
}
