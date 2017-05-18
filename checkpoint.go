package gorocksdb

// #include "rocksdb/c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

// Cache is a cache used to store data read from data in memory.
type Checkpoint struct {
	c *C.rocksdb_checkpoint_t
}

// NewLRUCache creates a new LRU Cache object with the capacity given.
func NewCheckpoint(db *DB) (*Checkpoint, error) {
	var cErr *C.char
	cCheckpoint := C.rocksdb_checkpoint_object_create(db.c, &cErr)

	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return NewNativeCheckpoint(cCheckpoint), nil
}

// NewNativeCheckpoint creates a Checkpoint object.
func NewNativeCheckpoint(c *C.rocksdb_checkpoint_t) *Checkpoint {
	return &Checkpoint{c}
}

// CreateCheckpoint creates the actual checkpoint in the specified directory.
// The logSizeForFlush argument is used the maximum size (in bytes) of the WAL
// files that will be copied. If the size of the WAL files is larger than the
// specfied size the memtables will be flushed to disk before making the
// checkpoint. Otherwise the WAL files will simply be copied.
func (c *Checkpoint) CreateCheckpoint(dir string, logSizeForFlush uint64) error {
	var cErr *C.char
	C.rocksdb_checkpoint_create(c.c, C.CString(dir), C.uint64_t(logSizeForFlush), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Destroy deallocates the Cache object.
func (c *Checkpoint) Destroy() {
	C.rocksdb_checkpoint_object_destroy(c.c)
	c.c = nil
}
