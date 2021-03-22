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

// SetIgnoreMissingColumnFamilies specifies the value of "ignore_missing_column_families".
// If true and if user is trying to write to column families that don't exist
// (they were dropped),  ignore the write (don't return an error). If there
// are multiple writes in a WriteBatch, other writes will succeed.
// Default: false
func (opts *WriteOptions) SetIgnoreMissingColumnFamilies(value bool) {
	C.rocksdb_writeoptions_set_ignore_missing_column_families(opts.c, boolToChar(value))
}

// SetNoSlowdown specifies the value of "no_slowdown".
// If true and we need to wait or sleep for the write request, fails
// immediately with Status::Incomplete().
// Default: false
func (opts *WriteOptions) SetNoSlowdown(value bool) {
	C.rocksdb_writeoptions_set_no_slowdown(opts.c, boolToChar(value))
}

// SetLowPri specifies the value of "low_pri".
// If true, this write request is of lower priority if compaction is
// behind. In this case, no_slowdown = true, the request will be cancelled
// immediately with Status::Incomplete() returned. Otherwise, it will be
// slowed down. The slowdown value is determined by RocksDB to guarantee
// it introduces minimum impacts to high priority writes.
//
// Default: false
func (opts *WriteOptions) SetLowPri(value bool) {
	C.rocksdb_writeoptions_set_low_pri(opts.c, boolToChar(value))
}

// Destroy deallocates the WriteOptions object.
func (opts *WriteOptions) Destroy() {
	C.rocksdb_writeoptions_destroy(opts.c)
	opts.c = nil
}
