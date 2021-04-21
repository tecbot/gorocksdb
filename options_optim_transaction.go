package gorocksdb

// #include "rocksdb/c.h"
import "C"

// TransactionOptions represent all of the available options options for
// a transaction on the database.
type OptimisticTransactionOptions struct {
	c *C.rocksdb_optimistictransaction_options_t
}

// NewDefaultTransactionOptions creates a default TransactionOptions object.
func NewDefaultOptimisticTransactionOptions() *OptimisticTransactionOptions {
	return NewNativeOptimisticTransactionOptions(C.rocksdb_optimistictransaction_options_create())
}

// NewNativeTransactionOptions creates a TransactionOptions object.
func NewNativeOptimisticTransactionOptions(c *C.rocksdb_optimistictransaction_options_t) *OptimisticTransactionOptions {
	return &OptimisticTransactionOptions{c}
}

// SetSetSnapshot to true is the same as calling
// Transaction::SetSnapshot().
func (opts *OptimisticTransactionOptions) SetSetSnapshot(value bool) {
	C.rocksdb_optimistictransaction_options_set_set_snapshot(opts.c, boolToChar(value))
}

// Destroy deallocates the TransactionOptions object.
func (opts *OptimisticTransactionOptions) Destroy() {
	C.rocksdb_optimistictransaction_options_destroy(opts.c)
	opts.c = nil
}
