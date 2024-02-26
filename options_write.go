package gorocksdb

// #include "rocksdb/c.h"
import "C"

// WriteOptions represent all of the available options when writing to a
// database.
type WriteOptions struct {
	c *C.rocksdb_writeoptions_t
}

// NewDefaultWriteOptions creates a default WriteOptions object.
func NewDefaultWriteOptions() *WriteOptions {
	return NewNativeWriteOptions(C.rocksdb_writeoptions_create())
}

// NewNativeWriteOptions creates a WriteOptions object.
func NewNativeWriteOptions(c *C.rocksdb_writeoptions_t) *WriteOptions {
	return &WriteOptions{c}
}

// SetSync sets the sync mode. If true, the write will be flushed
// from the operating system buffer cache before the write is considered complete.
// If this flag is true, writes will be slower.
// Default: false
func (opts *WriteOptions) SetSync(value bool) {
	C.rocksdb_writeoptions_set_sync(opts.c, boolToChar(value))
}

// DisableWAL sets whether WAL should be active or not.
// If true, writes will not first go to the write ahead log,
// and the write may got lost after a crash.
// Default: false
func (opts *WriteOptions) DisableWAL(value bool) {
	C.rocksdb_writeoptions_disable_WAL(opts.c, C.int(btoi(value)))
}

// SetLowPri if true, this write request is of lower priority if compaction is
// behind. In this case, no_slowdown = true, the request will be cancelled
// immediately with Status::Incomplete() returned. Otherwise, it will be
// slowed down. The slowdown value is determined by RocksDB to guarantee
// it introduces minimum impacts to high priority writes.
//
// Default: false
func (opts *WriteOptions) SetLowPri(value bool) {
	C.rocksdb_writeoptions_set_low_pri(opts.c, boolToChar(value))
}

// IsLowPri returns if the write request is of lower priority if compaction is behind.
func (opts *WriteOptions) IsLowPri() bool {
	return charToBool(C.rocksdb_writeoptions_get_low_pri(opts.c))
}

// Destroy deallocates the WriteOptions object.
func (opts *WriteOptions) Destroy() {
	C.rocksdb_writeoptions_destroy(opts.c)
	opts.c = nil
}
