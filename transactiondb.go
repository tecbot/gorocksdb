package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"errors"
	"unsafe"
)

type TransactionDB struct {
	c                 *C.rocksdb_transactiondb_t
	name              string
	opts              *Options
	transactionDBOpts *TransactionDBOptions
}

// OpenDb opens a database with the specified options.
func OpenTransactionDb(opts *Options, transactionDBOpts *TransactionDBOptions, name string) (*TransactionDB, error) {
	var (
		cErr  *C.char
		cName = C.CString(name)
	)
	defer C.free(unsafe.Pointer(cName))
	db := C.rocksdb_transactiondb_open(opts.c, transactionDBOpts.c, cName, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return &TransactionDB{
		name:              name,
		c:                 db,
		opts:              opts,
		transactionDBOpts: transactionDBOpts,
	}, nil
}

// NewSnapshot creates a new snapshot of the database.
func (transactionDB *TransactionDB) NewSnapshot() *Snapshot {
	return NewNativeSnapshot(C.rocksdb_transactiondb_create_snapshot(transactionDB.c))
}

// ReleaseSnapshot releases the snapshot and its resources.
func (transactionDB *TransactionDB) ReleaseSnapshot(snapshot *Snapshot) {
	C.rocksdb_transactiondb_release_snapshot(transactionDB.c, snapshot.c)
	snapshot.c = nil
}

func (transactionDB *TransactionDB) TransactionBegin(
	opts *WriteOptions,
	transactionOpts *TransactionOptions,
	oldTransaction *Transaction,
) *Transaction {
	if oldTransaction != nil {
		return NewNativeTransaction(C.rocksdb_transaction_begin(transactionDB.c, opts.c, transactionOpts.c, oldTransaction.c))
	}

	return NewNativeTransaction(C.rocksdb_transaction_begin(transactionDB.c, opts.c, transactionOpts.c, nil))
}

// Get returns the data associated with the key from the database.
func (transactionDB *TransactionDB) Get(opts *ReadOptions, key []byte) (*Slice, error) {
	var (
		cErr    *C.char
		cValLen C.size_t
		cKey    = byteToChar(key)
	)
	cValue := C.rocksdb_transactiondb_get(transactionDB.c, opts.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return NewSlice(cValue, cValLen), nil
}

// Put writes data associated with a key to the database.
func (transactionDB *TransactionDB) Put(opts *WriteOptions, key, value []byte) error {
	var (
		cErr   *C.char
		cKey   = byteToChar(key)
		cValue = byteToChar(value)
	)
	C.rocksdb_transactiondb_put(transactionDB.c, opts.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Delete removes the data associated with the key from the database.
func (transactionDB *TransactionDB) Delete(opts *WriteOptions, key []byte) error {
	var (
		cErr *C.char
		cKey = byteToChar(key)
	)
	C.rocksdb_transactiondb_delete(transactionDB.c, opts.c, cKey, C.size_t(len(key)), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

func (transactionDB *TransactionDB) CheckpointObjectCreate() (*Checkpoint, error) {
	var (
		cErr *C.char
	)
	cCheckpoint := C.rocksdb_transactiondb_checkpoint_object_create(transactionDB.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}

	return NewNativeCheckpoint(cCheckpoint), nil
}

// Close closes the database.
func (transactionDB *TransactionDB) Close() {
	C.rocksdb_transactiondb_close(transactionDB.c)
	transactionDB.c = nil
}
