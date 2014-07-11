package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"errors"
	"reflect"
	"unsafe"
)

// File adds column family support for RocksDB. Will need version >= 3. If you don't want
// to use version >= 3, feel free to delete this file and go on your merry way.

// TODO I'll gladly put these in more appropriate file but not sure how big a deal backwards compat is.

// If true, missing column families will be automatically created.
// Default: false
func (self *Options) SetCreateMissingColumnFamilies(value bool) {
	C.rocksdb_options_set_create_missing_column_families(self.c, boolToChar(value))
}

// ColumnFamilyDescriptor is used to open or create column families in the db.
type ColumnFamilyDescriptor struct {
	Name string
	Opts *Options
}

// ColumnFamily is a reusable handle to a column family in a Rocksdb database.
type ColumnFamily struct {
	c *C.rocksdb_column_family_handle_t
}

// Frees memory of Column Family.
func (cf *ColumnFamily) Destroy() {
	C.rocksdb_column_family_handle_destroy(cf.c)
}

// ListColumnFamilies lists all the column families in the database specified at name.
func ListColumnFamilies(dbOpts *Options, name string) ([]string, error) {
	var len C.size_t
	var cErr *C.char
	cfs := C.rocksdb_list_column_families(dbOpts.c, stringToChar(name), &len, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}

	hdr := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(cfs)), Len: int(len), Cap: int(len)}
	cfSlice := *(*[]*C.char)(unsafe.Pointer(&hdr))

	names := make([]string, int(len))
	for i, nameChar := range cfSlice {
		names[i] = string(C.GoString(nameChar))
	}
	C.rocksdb_list_column_families_destroy(cfs, len)
	return names, nil
}

// OpenWithColumnFamilies is similar to OpenDb but all column families defined must be
// specified in this call.
func OpenDbWithColumnFamilies(dbOpts *Options, name string, cfds []ColumnFamilyDescriptor) (*DB, []*ColumnFamily, error) {
	var cErr *C.char
	len := len(cfds)
	cfNames, cfOpts := cfdsToNameOpts(cfds)
	handles := make([]*C.rocksdb_column_family_handle_t, len)

	db := C.rocksdb_open_column_families(dbOpts.c, stringToChar(name),
		C.int(len), &cfNames[0], &cfOpts[0], &handles[0], &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, nil, errors.New(C.GoString(cErr))
	}

	cfs := make([]*ColumnFamily, len)
	for i, h := range handles {
		cfs[i] = &ColumnFamily{c: h}
	}

	return &DB{name: name, c: db, opts: dbOpts}, cfs, nil
}

// OpenDbForReadOnlyWithColumnFamilies opens the db with the column families specified for readonly.
func OpenDbForReadOnlyWithColumnFamilies(dbOpts *Options, name string, cfds []ColumnFamilyDescriptor, errorIfLogFileExist bool) (*DB, []*ColumnFamily, error) {
	var cErr *C.char
	len := len(cfds)
	cfNames, cfOpts := cfdsToNameOpts(cfds)
	handles := make([]*C.rocksdb_column_family_handle_t, len)

	db := C.rocksdb_open_for_read_only_column_families(dbOpts.c, stringToChar(name),
		C.int(len), &cfNames[0], &cfOpts[0], &handles[0], boolToChar(errorIfLogFileExist), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, nil, errors.New(C.GoString(cErr))
	}

	cfs := make([]*ColumnFamily, len)
	for i, h := range handles {
		cfs[i] = &ColumnFamily{c: h}
	}

	return &DB{name: name, c: db, opts: dbOpts}, cfs, nil
}

func cfdsToNameOpts(cfds []ColumnFamilyDescriptor) ([]*C.char, []*C.rocksdb_options_t) {
	cfNames := make([]*C.char, len(cfds))
	cfOpts := make([]*C.rocksdb_options_t, len(cfds))
	for i, cfd := range cfds {
		cfNames[i] = C.CString(cfd.Name)
		cfOpts[i] = cfd.Opts.c
	}
	return cfNames, cfOpts
}

// CreateColumnFamily creates a new column family in db.
func (self *DB) CreateColumnFamily(cfd ColumnFamilyDescriptor) (*ColumnFamily, error) {
	var cErr *C.char

	cf := C.rocksdb_create_column_family(self.c, cfd.Opts.c, stringToChar(cfd.Name), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return &ColumnFamily{c: cf}, nil
}

// Drops the column family from the db. Still should call cf.Destroy()
func (self *DB) DropColumnFamily(cf *ColumnFamily) error {
	var cErr *C.char
	C.rocksdb_drop_column_family(self.c, cf.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// PutCF writes data associated with a key to a column family in the database.
func (self *DB) PutCF(opts *WriteOptions, cf *ColumnFamily, key, value []byte) error {
	cKey := byteToChar(key)
	cValue := byteToChar(value)

	var cErr *C.char
	C.rocksdb_put_cf(self.c, opts.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// DeleteCF removes the data associated with key from the column family in the database.
func (self *DB) DeleteCF(opts *WriteOptions, cf *ColumnFamily, key []byte) error {
	cKey := byteToChar(key)

	var cErr *C.char
	C.rocksdb_delete_cf(self.c, opts.c, cf.c, cKey, C.size_t(len(key)), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}

	return nil
}

// MergeCF merges the data associated with the key with the actual data in the column family in the database.
func (self *DB) MergeCF(opts *WriteOptions, cf *ColumnFamily, key []byte, value []byte) error {
	cKey := byteToChar(key)
	cValue := byteToChar(value)

	var cErr *C.char
	C.rocksdb_merge_cf(self.c, opts.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}

	return nil
}

// GetCF returns the data associated with the key from the column family in the database.
func (self *DB) GetCF(opts *ReadOptions, cf *ColumnFamily, key []byte) (*Slice, error) {
	cKey := byteToChar(key)

	var cErr *C.char
	var cValLen C.size_t
	cValue := C.rocksdb_get_cf(self.c, opts.c, cf.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}

	return NewSlice(cValue, cValLen), nil
}

// NewIteratorCF returns an Iterator over the column family in the database
// that uses the given read options.
func (self *DB) NewIteratorCF(opts *ReadOptions, cf *ColumnFamily) *Iterator {
	cIter := C.rocksdb_create_iterator_cf(self.c, opts.c, cf.c)
	return NewNativeIterator(cIter)
}

// GetPropertyCF returns the value of a database property for a column family.
func (self *DB) GetPropertyCF(cf *ColumnFamily, propName string) string {
	cValue := C.rocksdb_property_value_cf(self.c, cf.c, stringToChar(propName))
	defer C.free(unsafe.Pointer(cValue))
	return C.GoString(cValue)
}

// GetApproximateSizes returns the approximate number of bytes of file system
// space used by one or more key ranges in the column family.
//
// The keys counted will begin at Range.Start and end on the key before
// Range.Limit.
func (self *DB) GetApproximateSizesCF(cf *ColumnFamily, ranges []Range) []uint64 {
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

	C.rocksdb_approximate_sizes_cf(self.c, cf.c, C.int(len(ranges)), &cStarts[0], &cStartLens[0], &cLimits[0], &cLimitLens[0], (*C.uint64_t)(&sizes[0]))
	return sizes
}

// CompactRange runs a manual compaction on the Range of keys given. This is
// not likely to be needed for typical usage.
func (self *DB) CompactRangeCF(cf *ColumnFamily, r Range) {
	cStart := byteToChar(r.Start)
	cLimit := byteToChar(r.Limit)

	C.rocksdb_compact_range_cf(self.c, cf.c, cStart, C.size_t(len(r.Start)), cLimit, C.size_t(len(r.Limit)))
}

// Put queues a key-value pair in the column family.
func (self *WriteBatch) PutCF(cf *ColumnFamily, key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)

	C.rocksdb_writebatch_put_cf(self.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// Merge queues a merge of "value" with the existing value of "key" in the column family.
func (self *WriteBatch) MergeCF(cf *ColumnFamily, key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)

	C.rocksdb_writebatch_merge_cf(self.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// Delete queues a deletion of the data at key from the column family.
func (self *WriteBatch) DeleteCF(cf *ColumnFamily, key []byte) {
	cKey := byteToChar(key)

	C.rocksdb_writebatch_delete_cf(self.c, cf.c, cKey, C.size_t(len(key)))
}
