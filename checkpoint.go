package gorocksdb

// #include "rocksdb/c.h"
import "C"

type Checkpoint struct {
	c *C.rocksdb_checkpoint_t
}

func NewNativeCheckpoint(c *C.rocksdb_checkpoint_t) *Checkpoint {
	return &Checkpoint{c}
}
