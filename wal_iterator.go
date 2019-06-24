package gorocksdb
// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"unsafe"
)

type WalIterator struct {
	c *C.rocksdb_wal_iterator_t
}

func NewNativeWalIterator(c unsafe.Pointer) *WalIterator {
	return &WalIterator{(*C.rocksdb_wal_iterator_t)(c)}
}

func (iter *WalIterator) Valid() bool {
	return C.rocksdb_wal_iter_valid(iter.c) != 0
}

func (iter *WalIterator) Next() {
	C.rocksdb_wal_iter_next(iter.c)
}

func (iter *WalIterator) Status() string {
	var cErr  *C.char
	C.rocksdb_wal_iter_status(iter.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return C.GoString(cErr)
	}
	return "unknown"
}

func (iter *WalIterator) Destroy() {
	C.rocksdb_wal_iter_destroy(iter.c)
	iter.c = nil
}

func (iter *WalIterator) Batch() (*WriteBatch, uint64) {
	var cSeq C.uint64_t
	cB := C.rocksdb_wal_iter_get_batch(iter.c, &cSeq)
	return NewNativeWriteBatch(cB), uint64(cSeq)
}