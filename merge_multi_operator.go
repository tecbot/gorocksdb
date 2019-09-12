package gorocksdb

// #include "rocksdb/c.h"
import "C"

// MergeMultiOperator is similar to a MergeOperator but implements PartialMergeMulti
// When given a MergeMultiOperator, the PartialMergeMulti will always
// be used instead of PartialMerge
type MergeMultiOperator interface {
	MergeOperator
	// PartialMerge performs merge on multiple operands
	// when all of the operands are themselves merge operation types
	// that you would have passed to a db.Merge() call in the same order
	// (i.e.: db.Merge(key,operand[0]), followed by db.Merge(key,operand[1]),
	// ... db.Merge(key, operand[n])).
	//
	// PartialMerge should combine them into a single merge operation.
	// The return value should be constructed such that a call to
	// db.Merge(key, new_value) would yield the same result as a call
	// to db.Merge(key,operand[0]), followed by db.Merge(key,operand[1]),
	// ... db.Merge(key, operand[n])).
	//
	// If it is impossible or infeasible to combine the operations, return false.
	// The library will internally keep track of the operations, and apply them in the
	// correct order once a base-value (a Put/Delete/End-of-Database) is seen.
	PartialMergeMulti(key []byte, operands [][]byte) ([]byte, bool)
}
