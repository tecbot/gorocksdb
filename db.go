package gorocksdb

// #cgo LDFLAGS: -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy
// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"

import (
	"errors"
	"unsafe"
)

// Range is a range of keys in the database. GetApproximateSizes calls with it
// begin at the key Start and end right before the key Limit.
type Range struct {
	Start []byte
	Limit []byte
}

// DB is a reusable handle to a RocksDB database on disk, created by Open.
type DB struct {
	c    *C.rocksdb_t
	name string
	opts *Options
}

// OpenDb opens a database with the specified options.
func OpenDb(opts *Options, name string) (*DB, error) {
	var cErr *C.char
	db := C.rocksdb_open(opts.c, StringToChar(name), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))

		return nil, errors.New(C.GoString(cErr))
	}

	return &DB{
		name: name,
		c:    db,
		opts: opts,
	}, nil
}

// Name returns the name of the database.
func (self *DB) Name() string {
	return self.name
}

// Get returns the data associated with the key from the database.
func (self *DB) Get(opts *ReadOptions, key []byte) (*Slice, error) {
	cKey := ByteToChar(key)

	var cErr *C.char
	var cValLen C.size_t
	cValue := C.rocksdb_get(self.c, opts.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))

		return nil, errors.New(C.GoString(cErr))
	}

	return NewSlice(cValue, cValLen), nil
}

// Put writes data associated with a key to the database.
func (self *DB) Put(opts *WriteOptions, key, value []byte) error {
	cKey := ByteToChar(key)
	cValue := ByteToChar(value)

	var cErr *C.char
	C.rocksdb_put(self.c, opts.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))

		return errors.New(C.GoString(cErr))
	}

	return nil
}

// Delete removes the data associated with the key from the database.
func (self *DB) Delete(opts *WriteOptions, key []byte) error {
	cKey := ByteToChar(key)

	var cErr *C.char
	C.rocksdb_delete(self.c, opts.c, cKey, C.size_t(len(key)), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))

		return errors.New(C.GoString(cErr))
	}

	return nil
}

// Merge merges the data associated with the key with the actual data in the database.
func (self *DB) Merge(opts *WriteOptions, key []byte, value []byte) error {
	cKey := ByteToChar(key)
	cValue := ByteToChar(value)

	var cErr *C.char
	C.rocksdb_merge(self.c, opts.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))

		return errors.New(C.GoString(cErr))
	}

	return nil
}

// Write writes a WriteBatch to the database
func (self *DB) Write(opts *WriteOptions, batch *WriteBatch) error {
	var cErr *C.char
	C.rocksdb_write(self.c, opts.c, batch.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))

		return errors.New(C.GoString(cErr))
	}

	return nil
}

// NewIterator returns an Iterator over the the database that uses the
// ReadOptions given.
func (self *DB) NewIterator(opts *ReadOptions) *Iterator {
	cIter := C.rocksdb_create_iterator(self.c, opts.c)

	return NewNativeIterator(cIter)
}

// NewSnapshot creates a new snapshot of the database.
func (self *DB) NewSnapshot() *Snapshot {
	cSnap := C.rocksdb_create_snapshot(self.c)

	return NewNativeSnapshot(cSnap, self.c)
}

// GetProperty returns the value of a database property.
func (self *DB) GetProperty(propName string) string {
	cValue := C.rocksdb_property_value(self.c, StringToChar(propName))
	defer C.free(unsafe.Pointer(cValue))

	return C.GoString(cValue)
}

// GetApproximateSizes returns the approximate number of bytes of file system
// space used by one or more key ranges.
//
// The keys counted will begin at Range.Start and end on the key before
// Range.Limit.
func (self *DB) GetApproximateSizes(ranges []Range) []uint64 {
	sizes := make([]uint64, len(ranges))
	if len(ranges) == 0 {
		return sizes
	}

	cStarts := make([]*C.char, len(ranges))
	cLimits := make([]*C.char, len(ranges))
	cStartLens := make([]C.size_t, len(ranges))
	cLimitLens := make([]C.size_t, len(ranges))
	for i, r := range ranges {
		cStarts[i] = ByteToChar(r.Start)
		cStartLens[i] = C.size_t(len(r.Start))
		cLimits[i] = ByteToChar(r.Limit)
		cLimitLens[i] = C.size_t(len(r.Limit))
	}

	C.rocksdb_approximate_sizes(self.c, C.int(len(ranges)), &cStarts[0], &cStartLens[0], &cLimits[0], &cLimitLens[0], (*C.uint64_t)(&sizes[0]))

	return sizes
}

// CompactRange runs a manual compaction on the Range of keys given. This is
// not likely to be needed for typical usage.
func (self *DB) CompactRange(r Range) {
	cStart := ByteToChar(r.Start)
	cLimit := ByteToChar(r.Limit)

	C.rocksdb_compact_range(self.c, cStart, C.size_t(len(r.Start)), cLimit, C.size_t(len(r.Limit)))
}

// Flush triggers a manuel flush for the database.
func (self *DB) Flush(opts *FlushOptions) error {
	var cErr *C.char
	C.rocksdb_flush(self.c, opts.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))

		return errors.New(C.GoString(cErr))
	}

	return nil
}

// DisableFileDeletions disables file deletions and should be used when backup the database.
func (self *DB) DisableFileDeletions() error {
	var cErr *C.char
	C.rocksdb_disable_file_deletions(self.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))

		return errors.New(C.GoString(cErr))
	}

	return nil
}

// EnableFileDeletions enables file deletions for the database.
func (self *DB) EnableFileDeletions(force bool) error {
	var cErr *C.char
	C.rocksdb_enable_file_deletions(self.c, BoolToChar(force), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))

		return errors.New(C.GoString(cErr))
	}

	return nil
}

// Close closes the database.
func (self *DB) Close() {
	C.rocksdb_close(self.c)
}

// DestroyDb removes a database entirely, removing everything from the
// filesystem.
func DestroyDb(name string, opts *Options) error {
	var cErr *C.char
	C.rocksdb_destroy_db(opts.c, StringToChar(name), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))

		return errors.New(C.GoString(cErr))
	}

	return nil
}

// RepairDb repairs a database.
func RepairDb(name string, opts *Options) error {
	var cErr *C.char
	C.rocksdb_destroy_db(opts.c, StringToChar(name), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))

		return errors.New(C.GoString(cErr))
	}

	return nil
}
