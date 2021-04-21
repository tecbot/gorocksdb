package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"errors"
	"unsafe"
)

// OptimisticTransactionDB is a reusable handle to a RocksDB optimistic transactional database on disk, created by OpenOptimisticTransactionDb.
type OptimisticTransactionDB struct {
	c    *C.rocksdb_optimistictransactiondb_t
	name string
	opts *Options
}

// OpenOptimisticTransactionDb opens a database with the specified options.
func OpenOptimisticTransactionDb(opts *Options, name string) (*OptimisticTransactionDB, error) {
	var (
		cErr  *C.char
		cName = C.CString(name)
	)
	defer C.free(unsafe.Pointer(cName))
	db := C.rocksdb_optimistictransactiondb_open(
		opts.c, cName, &cErr)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return &OptimisticTransactionDB{
		name: name,
		c:    db,
		opts: opts,
	}, nil
}

// GetBaseDb returns the handle to the underlying DB instance.
func (db *OptimisticTransactionDB) GetBaseDb() *DB {
	baseDb := C.rocksdb_optimistictransactiondb_get_base_db(db.c)
	return &DB{
		name: db.name,
		c:    baseDb,
		opts: db.opts,
	}
}

// TransactionBegin begins a new transaction
// with the WriteOptions and TransactionOptions given.
func (db *OptimisticTransactionDB) TransactionBegin(
	opts *WriteOptions,
	transactionOpts *OptimisticTransactionOptions,
	oldTransaction *Transaction,
) *Transaction {
	if oldTransaction != nil {
		return NewNativeTransaction(C.rocksdb_optimistictransaction_begin(
			db.c,
			opts.c,
			transactionOpts.c,
			oldTransaction.c,
		))
	}

	return NewNativeTransaction(C.rocksdb_optimistictransaction_begin(
		db.c, opts.c, transactionOpts.c, nil))
}

// Close closes the database.
func (transactionDB *OptimisticTransactionDB) Close() {
	C.rocksdb_optimistictransactiondb_close(transactionDB.c)
	transactionDB.c = nil
}
