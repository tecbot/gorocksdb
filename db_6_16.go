// +build rocksdb_6_16

package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"errors"
	"unsafe"
)

// GetApproximateSizes returns the approximate number of bytes of file system
// space used by one or more key ranges.
//
// The keys counted will begin at Range.Start and end on the key before
// Range.Limit.
func (db *DB) GetApproximateSizes(ranges []Range) (sizes []uint64, err error) {
	var cErr *C.char
	sizes = make([]uint64, len(ranges))
	if len(ranges) == 0 {
		return
	}

	cStarts := make([]*C.char, len(ranges))
	cLimits := make([]*C.char, len(ranges))
	cStartLens := make([]C.size_t, len(ranges))
	cLimitLens := make([]C.size_t, len(ranges))
	for i, r := range ranges {
		cStarts[i] = (*C.char)(C.CBytes(r.Start))
		cStartLens[i] = C.size_t(len(r.Start))
		cLimits[i] = (*C.char)(C.CBytes(r.Limit))
		cLimitLens[i] = C.size_t(len(r.Limit))
	}

	defer func() {
		for i := range ranges {
			C.free(unsafe.Pointer(cStarts[i]))
			C.free(unsafe.Pointer(cLimits[i]))
		}
	}()

	C.rocksdb_approximate_sizes(
		db.c,
		C.int(len(ranges)),
		&cStarts[0],
		&cStartLens[0],
		&cLimits[0],
		&cLimitLens[0],
		(*C.uint64_t)(&sizes[0]),
		&cErr,
	)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		err = errors.New(C.GoString(cErr))
	}

	return
}

// GetApproximateSizesCF returns the approximate number of bytes of file system
// space used by one or more key ranges in the column family.
//
// The keys counted will begin at Range.Start and end on the key before
// Range.Limit.
func (db *DB) GetApproximateSizesCF(cf *ColumnFamilyHandle, ranges []Range) (sizes []uint64, err error) {
	var cErr *C.char
	sizes = make([]uint64, len(ranges))
	if len(ranges) == 0 {
		return
	}

	cStarts := make([]*C.char, len(ranges))
	cLimits := make([]*C.char, len(ranges))
	cStartLens := make([]C.size_t, len(ranges))
	cLimitLens := make([]C.size_t, len(ranges))
	for i, r := range ranges {
		cStarts[i] = (*C.char)(C.CBytes(r.Start))
		cStartLens[i] = C.size_t(len(r.Start))
		cLimits[i] = (*C.char)(C.CBytes(r.Limit))
		cLimitLens[i] = C.size_t(len(r.Limit))
	}

	defer func() {
		for i := range ranges {
			C.free(unsafe.Pointer(cStarts[i]))
			C.free(unsafe.Pointer(cLimits[i]))
		}
	}()

	C.rocksdb_approximate_sizes_cf(
		db.c,
		cf.c,
		C.int(len(ranges)),
		&cStarts[0],
		&cStartLens[0],
		&cLimits[0],
		&cLimitLens[0],
		(*C.uint64_t)(&sizes[0]),
		&cErr,
	)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		err = errors.New(C.GoString(cErr))
	}

	return
}
