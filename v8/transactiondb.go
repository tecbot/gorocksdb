package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"errors"
	"unsafe"
)

// TransactionDB is a reusable handle to a RocksDB transactional database on disk, created by OpenTransactionDb.
type TransactionDB struct {
	c                 *C.rocksdb_transactiondb_t
	name              string
	opts              *Options
	transactionDBOpts *TransactionDBOptions
}

// OpenTransactionDb opens a database with the specified options.
func OpenTransactionDb(
	opts *Options,
	transactionDBOpts *TransactionDBOptions,
	name string,
) (*TransactionDB, error) {
	var (
		cErr  *C.char
		cName = C.CString(name)
	)
	defer C.free(unsafe.Pointer(cName))
	db := C.rocksdb_transactiondb_open(
		opts.c, transactionDBOpts.c, cName, &cErr)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return &TransactionDB{
		name:              name,
		c:                 db,
		opts:              opts,
		transactionDBOpts: transactionDBOpts,
	}, nil
}

// OpenDbColumnFamilies opens a database with the specified column families.
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

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

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

	cHandles := make([]*C.rocksdb_column_family_handle_t, numColumnFamilies)

	var cErr *C.char
	db := C.rocksdb_transactiondb_open_column_families(
		opts.c,
		transactionDBOpts.c,
		cName,
		C.int(numColumnFamilies),
		&cNames[0],
		&cOpts[0],
		&cHandles[0],
		&cErr,
	)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return nil, nil, errors.New(C.GoString(cErr))
	}

	cfHandles := make([]*ColumnFamilyHandle, numColumnFamilies)
	for i, c := range cHandles {
		cfHandles[i] = NewNativeColumnFamilyHandle(c)
	}

	return &TransactionDB{
		name: name,
		c:    db,
		opts: opts,
	}, cfHandles, nil
}

// CreateColumnFamily creates a new column family.
func (db *TransactionDB) CreateColumnFamily(opts *Options, name string) (*ColumnFamilyHandle, error) {
	var (
		cErr  *C.char
		cName = C.CString(name)
	)
	defer C.free(unsafe.Pointer(cName))
	cHandle := C.rocksdb_transactiondb_create_column_family(db.c, opts.c, cName, &cErr)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return NewNativeColumnFamilyHandle(cHandle), nil
}

// NewSnapshot creates a new snapshot of the database.
func (db *TransactionDB) NewSnapshot() *Snapshot {
	return NewNativeSnapshot(C.rocksdb_transactiondb_create_snapshot(db.c))
}

// ReleaseSnapshot releases the snapshot and its resources.
func (db *TransactionDB) ReleaseSnapshot(snapshot *Snapshot) {
	C.rocksdb_transactiondb_release_snapshot(db.c, snapshot.c)
	snapshot.c = nil
}

// GetBaseDB gets base db.
func (db *TransactionDB) GetBaseDB() *DB {
	base := C.rocksdb_transactiondb_get_base_db(db.c)
	return &DB{c: base}
}

// CloseBaseDBOfTransactionDB closes base db of TransactionDB.
func CloseBaseDBOfTransactionDB(db *DB) {
	if db != nil && db.c != nil {
		C.rocksdb_transactiondb_close_base_db(db.c)
	}
}

// TransactionBegin begins a new transaction
// with the WriteOptions and TransactionOptions given.
func (db *TransactionDB) TransactionBegin(
	opts *WriteOptions,
	transactionOpts *TransactionOptions,
	oldTransaction *Transaction,
) *Transaction {
	if oldTransaction != nil {
		return NewNativeTransaction(C.rocksdb_transaction_begin(
			db.c,
			opts.c,
			transactionOpts.c,
			oldTransaction.c,
		))
	}

	return NewNativeTransaction(C.rocksdb_transaction_begin(
		db.c, opts.c, transactionOpts.c, nil))
}

// Get returns the data associated with the key from the database.
func (db *TransactionDB) Get(opts *ReadOptions, key []byte) (*Slice, error) {
	var (
		cErr    *C.char
		cValLen C.size_t
		cKey    = byteToChar(key)
	)
	cValue := C.rocksdb_transactiondb_get(
		db.c, opts.c, cKey, C.size_t(len(key)), &cValLen, &cErr,
	)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return NewSlice(cValue, cValLen), nil
}

// GetCF returns the data associated with the key in a given column family from the database.
func (db *TransactionDB) GetCF(opts *ReadOptions, cf *ColumnFamilyHandle, key []byte) (*Slice, error) {
	var (
		cErr    *C.char
		cValLen C.size_t
		cKey    = byteToChar(key)
	)
	cValue := C.rocksdb_transactiondb_get_cf(db.c, opts.c, cf.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return NewSlice(cValue, cValLen), nil

}

// Put writes data associated with a key to the database.
func (db *TransactionDB) Put(opts *WriteOptions, key, value []byte) error {
	var (
		cErr   *C.char
		cKey   = byteToChar(key)
		cValue = byteToChar(value)
	)
	C.rocksdb_transactiondb_put(
		db.c, opts.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), &cErr,
	)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// PutCF writes data associated with a key to the database and column family.
func (db *TransactionDB) PutCF(opts *WriteOptions, cf *ColumnFamilyHandle, key, value []byte) error {
	var (
		cErr   *C.char
		cKey   = byteToChar(key)
		cValue = byteToChar(value)
	)
	C.rocksdb_transactiondb_put_cf(db.c, opts.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), &cErr)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Write writes a WriteBatch to the database
func (db *TransactionDB) Write(opts *WriteOptions, batch *WriteBatch) error {
	var cErr *C.char
	C.rocksdb_transactiondb_write(db.c, opts.c, batch.c, &cErr)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Delete removes the data associated with the key from the database.
func (db *TransactionDB) Delete(opts *WriteOptions, key []byte) error {
	var (
		cErr *C.char
		cKey = byteToChar(key)
	)
	C.rocksdb_transactiondb_delete(db.c, opts.c, cKey, C.size_t(len(key)), &cErr)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// DeleteCF removes the data associated with the key from the database and column family.
func (db *TransactionDB) DeleteCF(opts *WriteOptions, cf *ColumnFamilyHandle, key []byte) error {
	var (
		cErr *C.char
		cKey = byteToChar(key)
	)
	C.rocksdb_transactiondb_delete_cf(db.c, opts.c, cf.c, cKey, C.size_t(len(key)), &cErr)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// NewCheckpoint creates a new Checkpoint for this db.
func (db *TransactionDB) NewCheckpoint() (*Checkpoint, error) {
	var (
		cErr *C.char
	)
	cCheckpoint := C.rocksdb_transactiondb_checkpoint_object_create(
		db.c, &cErr,
	)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}

	return NewNativeCheckpoint(cCheckpoint), nil
}

// NewIterator returns an Iterator over the database that uses the
// ReadOptions given.
func (db *TransactionDB) NewIterator(opts *ReadOptions) *Iterator {
	return NewNativeIterator(unsafe.Pointer(C.rocksdb_transactiondb_create_iterator(db.c, opts.c)))
}

// NewIteratorCF returns an Iterator over the column family that uses the
// ReadOptions given.
func (db *TransactionDB) NewIteratorCF(opts *ReadOptions, cf *ColumnFamilyHandle) *Iterator {
	return NewNativeIterator(unsafe.Pointer(C.rocksdb_transactiondb_create_iterator_cf(db.c, opts.c, cf.c)))
}

// UnsafeGetDB returns the underlying c rocksdb instance.
func (db *TransactionDB) UnsafeGetDB() unsafe.Pointer {
	return unsafe.Pointer(db.c)
}

// Close closes the database.
func (db *TransactionDB) Close() {
	C.rocksdb_transactiondb_close(db.c)
	db.c = nil
}
