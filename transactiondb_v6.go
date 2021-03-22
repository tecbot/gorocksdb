// +build rocksdb_v6

package gorocksdb

import (
	"errors"
)

import "C"

// OpenTransactionDbColumnFamilies opens a database with the specified options.
func OpenTransactionDbColumnFamilies(
	opts *Options,
	transactionDBOpts *TransactionDBOptions,
	name string,
	cfNames []string,
	cfOpts []*Options,
) (*TransactionDB, []*ColumnFamilyHandle, error) {
	numColumnFamilies := len(cfNames)
	if numColumnFamilies != len(cfOpts) {
		return nil, nil, errors.New("must provide the same number of column family names and options")
	}
	cNames := make([]*C.char, numColumnFamilies)
	for i, s := range cfNames {
		cNames[i] = C.CString(s)
	}
	defer func() {
		for _, s := range cNames {
			C.free(unsafe.Pointer(s))
		}
	}()

	cOpts := make([]*C.rocksdb_options_t, numColumnFamilies)
	for i, o := range cfOpts {
		cOpts[i] = o.c
	}

	var (
		cErr  *C.char
		cName = C.CString(name)
	)
	defer C.free(unsafe.Pointer(cName))

	cHandles := make([]*C.rocksdb_column_family_handle_t, numColumnFamilies)

	db := C.rocksdb_transactiondb_open_column_families(
		opts.c,
		transactionDBOpts.c,
		cName,
		C.int(numColumnFamilies),
		&cNames[0],
		&cOpts[0],
		&cHandles[0],
		&cErr)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return nil, nil, errors.New(C.GoString(cErr))
	}

	cfHandles := make([]*ColumnFamilyHandle, numColumnFamilies)
	for i, c := range cHandles {
		cfHandles[i] = NewNativeColumnFamilyHandle(c)
	}

	return &TransactionDB{
		name:              name,
		c:                 db,
		opts:              opts,
		transactionDBOpts: transactionDBOpts,
	}, cfHandles, nil
}

// NewIterator returns an Iterator over the database that uses the
// ReadOptions given and column family.
func (db *TransactionDB) NewIteratorCF(opts *ReadOptions, cf *ColumnFamilyHandle) *Iterator {
	return NewNativeIterator(
		unsafe.Pointer(C.rocksdb_transactiondb_create_iterator_cf(db.c, opts.c, cf.c)))
}
