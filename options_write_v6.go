// +build rocksdb_v6

package gorocksdb

import "C"

// SetMemtableInsertHintPerBatch specifies the value of "memtable_insert_hint_per_batch".
// If true, this writebatch will maintain the last insert positions of each
// memtable as hints in concurrent write. It can improve write performance
// in concurrent writes if keys in one writebatch are sequential. In
// non-concurrent writes (when concurrent_memtable_writes is false) this
// option will be ignored.
//
// Default: false
func (opts *WriteOptions) SetMemtableInsertHintPerBatch(value bool) {
	C.rocksdb_writeoptions_set_memtable_insert_hint_per_batch(opts.c, boolToChar(value))
}
