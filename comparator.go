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
	Compare(a []byte, b []byte) int

	// The name of the comparator.
	Name() string
}

// NewComparator creates a new comparator for the given handler.
func NewComparator(handler ComparatorHandler) *Comparator {
	return NewNativeComparator(C.gorocksdb_comparator_create(unsafe.Pointer(&handler)))
}

// NewNativeComparator allocates a Comparator object.
func NewNativeComparator(c *C.rocksdb_comparator_t) *Comparator {
	return &Comparator{c}
}

// Destroy deallocates the Comparator object.
func (self *Comparator) Destroy() {
	C.rocksdb_comparator_destroy(self.c)
}

//export gorocksdb_comparator_compare
func gorocksdb_comparator_compare(cHandler unsafe.Pointer, cKeyA *C.char, cKeyALen C.size_t, cKeyB *C.char, cKeyBLen C.size_t) C.int {
	keyA := CharToByte(cKeyA, cKeyALen)
	keyB := CharToByte(cKeyB, cKeyBLen)

	var handler ComparatorHandler = *(*ComparatorHandler)(cHandler)
	compare := handler.Compare(keyA, keyB)

	return C.int(compare)
}

//export gorocksdb_comparator_name
func gorocksdb_comparator_name(cHandler unsafe.Pointer) *C.char {
	var handler ComparatorHandler = *(*ComparatorHandler)(cHandler)

	return StringToChar(handler.Name())
}
