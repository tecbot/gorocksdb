package gorocksdb

// #cgo CXXFLAGS: -lstdc++ -lm -std=c++11 -w
// #cgo LDFLAGS: -lstdc++ -lm
// #include <stdlib.h>
// #include "rocksdb/c.h"
// #define ROCKSDB_PLATFORM_POSIX
import "C"

// TODO snappy w/o make install'd working?
// OLD FLAGS BELOW
// #cgo LDFLAGS: -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy
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

// Metadata associated with each SST file.
type LiveFileMetadata struct {
	Name        string
	Level       int
	Size        int64
	SmallestKey []byte
	LargestKey  []byte
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
	db := C.rocksdb_open(opts.c, stringToChar(name), &cErr)
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

// OpenDbForReadOnly opens a database with the specified options for readonly usage.
func OpenDbForReadOnly(opts *Options, name string, errorIfLogFileExist bool) (*DB, error) {
	var cErr *C.char
	db := C.rocksdb_open_for_read_only(opts.c, stringToChar(name), boolToChar(errorIfLogFileExist), &cErr)
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
	cKey := byteToChar(key)

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
	cKey := byteToChar(key)
	cValue := byteToChar(value)

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
	cKey := byteToChar(key)

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
	cKey := byteToChar(key)
	cValue := byteToChar(value)

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
	cValue := C.rocksdb_property_value(self.c, stringToChar(propName))
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
		cStarts[i] = byteToChar(r.Start)
		cStartLens[i] = C.size_t(len(r.Start))
		cLimits[i] = byteToChar(r.Limit)
		cLimitLens[i] = C.size_t(len(r.Limit))
	}

	C.rocksdb_approximate_sizes(self.c, C.int(len(ranges)), &cStarts[0], &cStartLens[0], &cLimits[0], &cLimitLens[0], (*C.uint64_t)(&sizes[0]))

	return sizes
}

// GetLiveFilesMetaData returns a list of all table files with their
// level, start key and end key.
func (self *DB) GetLiveFilesMetaData() []LiveFileMetadata {
	lf := C.rocksdb_livefiles(self.c)
	defer C.rocksdb_livefiles_destroy(lf)

	count := C.rocksdb_livefiles_count(lf)
	liveFiles := make([]LiveFileMetadata, int(count))
	for i := C.int(0); i < count; i++ {
		var liveFile LiveFileMetadata
		liveFile.Name = C.GoString(C.rocksdb_livefiles_name(lf, i))
		liveFile.Level = int(C.rocksdb_livefiles_level(lf, i))
		liveFile.Size = int64(C.rocksdb_livefiles_size(lf, i))

		var cSize C.size_t
		key := C.rocksdb_livefiles_smallestkey(lf, i, &cSize)
		liveFile.SmallestKey = C.GoBytes(unsafe.Pointer(key), C.int(cSize))

		key = C.rocksdb_livefiles_largestkey(lf, i, &cSize)
		liveFile.LargestKey = C.GoBytes(unsafe.Pointer(key), C.int(cSize))
		liveFiles[int(i)] = liveFile
	}

	return liveFiles
}

// CompactRange runs a manual compaction on the Range of keys given. This is
// not likely to be needed for typical usage.
func (self *DB) CompactRange(r Range) {
	cStart := byteToChar(r.Start)
	cLimit := byteToChar(r.Limit)

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
	C.rocksdb_enable_file_deletions(self.c, boolToChar(force), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))

		return errors.New(C.GoString(cErr))
	}

	return nil
}

// Delete the file name from the db directory and update the internal state to
// reflect that. Supports deletion of sst and log files only. 'name' must be
// path relative to the db directory. eg. 000001.sst, /archive/000003.log.
func (self *DB) DeleteFile(name string) {
	C.rocksdb_delete_file(self.c, stringToChar(name))
}

// Close closes the database.
func (self *DB) Close() {
	C.rocksdb_close(self.c)
}

// DestroyDb removes a database entirely, removing everything from the
// filesystem.
func DestroyDb(name string, opts *Options) error {
	var cErr *C.char
	C.rocksdb_destroy_db(opts.c, stringToChar(name), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))

		return errors.New(C.GoString(cErr))
	}

	return nil
}

// RepairDb repairs a database.
func RepairDb(name string, opts *Options) error {
	var cErr *C.char
	C.rocksdb_repair_db(opts.c, stringToChar(name), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))

		return errors.New(C.GoString(cErr))
	}

	return nil
}
