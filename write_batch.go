package gorocksdb

// #include "rocksdb/c.h"
import "C"
import (
	"errors"
	"io"
)

// WriteBatch is a batching of Puts, Merges and Deletes.
type WriteBatch struct {
	c *C.rocksdb_writebatch_t
}

// NewWriteBatch create a WriteBatch object.
func NewWriteBatch() *WriteBatch {
	return NewNativeWriteBatch(C.rocksdb_writebatch_create())
}

// NewNativeWriteBatch create a WriteBatch object.
func NewNativeWriteBatch(c *C.rocksdb_writebatch_t) *WriteBatch {
	return &WriteBatch{c}
}

// WriteBatchFrom creates a write batch from a serialized WriteBatch.
func WriteBatchFrom(data []byte) *WriteBatch {
	return NewNativeWriteBatch(C.rocksdb_writebatch_create_from(byteToChar(data), C.size_t(len(data))))
}

// Put queues a key-value pair.
func (wb *WriteBatch) Put(key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	C.rocksdb_writebatch_put(wb.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// PutCF queues a key-value pair in a column family.
func (wb *WriteBatch) PutCF(cf *ColumnFamilyHandle, key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	C.rocksdb_writebatch_put_cf(wb.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// Merge queues a merge of "value" with the existing value of "key".
func (wb *WriteBatch) Merge(key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	C.rocksdb_writebatch_merge(wb.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// MergeCF queues a merge of "value" with the existing value of "key" in a
// column family.
func (wb *WriteBatch) MergeCF(cf *ColumnFamilyHandle, key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	C.rocksdb_writebatch_merge_cf(wb.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// Delete queues a deletion of the data at key.
func (wb *WriteBatch) Delete(key []byte) {
	cKey := byteToChar(key)
	C.rocksdb_writebatch_delete(wb.c, cKey, C.size_t(len(key)))
}

// DeleteCF queues a deletion of the data at key in a column family.
func (wb *WriteBatch) DeleteCF(cf *ColumnFamilyHandle, key []byte) {
	cKey := byteToChar(key)
	C.rocksdb_writebatch_delete_cf(wb.c, cf.c, cKey, C.size_t(len(key)))
}

// Data returns the serialized version of this batch.
func (wb *WriteBatch) Data() []byte {
	var cSize C.size_t
	cValue := C.rocksdb_writebatch_data(wb.c, &cSize)
	return charToByte(cValue, cSize)
}

// Count returns the number of updates in the batch.
func (wb *WriteBatch) Count() int {
	return int(C.rocksdb_writebatch_count(wb.c))
}

// NewIterator returns a iterator to iterate over the records in the batch.
func (wb *WriteBatch) NewIterator() *WriteBatchIterator {
	data := wb.Data()
	if len(data) < 8+4 {
		return &WriteBatchIterator{}
	}
	return &WriteBatchIterator{data: data[12:]}
}

// Clear removes all the enqueued Put and Deletes.
func (wb *WriteBatch) Clear() {
	C.rocksdb_writebatch_clear(wb.c)
}

// Destroy deallocates the WriteBatch object.
func (wb *WriteBatch) Destroy() {
	C.rocksdb_writebatch_destroy(wb.c)
	wb.c = nil
}

// WriteBatchRecordType describes the type of a batch record.
type WriteBatchRecordType byte

// Types of batch records.
const (
	WriteBatchRecordTypeDeletion   WriteBatchRecordType = 0x0
	WriteBatchRecordTypeValue      WriteBatchRecordType = 0x1
	WriteBatchRecordTypeMerge      WriteBatchRecordType = 0x2
	WriteBatchRecordTypeLogData    WriteBatchRecordType = 0x3
	WriteBatchRecordTypeCFDeletion WriteBatchRecordType = 0x4
	WriteBatchRecordTypeCFValue    WriteBatchRecordType = 0x5
	WriteBatchRecordTypeCFMerge    WriteBatchRecordType = 0x6
	WriteBatchRecordTypeSingleDeletion WriteBatchRecordType = 0x7
	WriteBatchRecordTypeCFSingleDeletion WriteBatchRecordType = 0x8
	WriteBatchRecordTypeNoop WriteBatchRecordType = 0xD
	WriteBatchRecordTypeBeginPrepareXID WriteBatchRecordType = 0x9
	WriteBatchRecordTypeEndPrepareXID WriteBatchRecordType = 0xA
	WriteBatchRecordTypeCommitXID WriteBatchRecordType = 0xB
	WriteBatchRecordTypeRollbackXID WriteBatchRecordType = 0xC
	WriteBatchRecordTypeNotUsed WriteBatchRecordType = 0x7F
)

// WriteBatchRecord represents a record inside a WriteBatch.
type WriteBatchRecord struct {
	CF    int
	Key   []byte
	Value []byte
	Type  WriteBatchRecordType
}

// WriteBatchIterator represents a iterator to iterator over records.
type WriteBatchIterator struct {
	data   []byte
	record WriteBatchRecord
	err    error
}

// Next returns the next record.
// Returns false if no further record exists.
func (iter *WriteBatchIterator) Next() bool {
	if iter.err != nil || len(iter.data) == 0 {
		return false
	}
	// reset the current record
	iter.record.Key = nil
	iter.record.Value = nil

	// parse the record type
	iter.record.Type = iter.decodeRecType()

	switch iter.record.Type {
	case WriteBatchRecordTypeDeletion, WriteBatchRecordTypeSingleDeletion,
		WriteBatchRecordTypeBeginPrepareXID, WriteBatchRecordTypeCommitXID,
		WriteBatchRecordTypeRollbackXID:
		iter.record.Key = iter.decodeSlice()
	case WriteBatchRecordTypeValue, WriteBatchRecordTypeMerge:
		iter.record.Key = iter.decodeSlice()
		if iter.err == nil {
			iter.record.Value = iter.decodeSlice()
		}
	case WriteBatchRecordTypeCFDeletion, WriteBatchRecordTypeCFValue,
		WriteBatchRecordTypeCFMerge, WriteBatchRecordTypeCFSingleDeletion:
		iter.record.CF = int(iter.decodeVarint())
		if iter.err == nil {
			iter.record.Key = iter.decodeSlice()
		}
		if iter.err == nil {
			iter.record.Value = iter.decodeSlice()
		}
	case WriteBatchRecordTypeEndPrepareXID, WriteBatchRecordTypeNoop,
		WriteBatchRecordTypeNotUsed:
	default:
		iter.err = errors.New("unsupported wal record type")
	}

	return iter.err == nil

}

// Record returns the current record.
func (iter *WriteBatchIterator) Record() *WriteBatchRecord {
	return &iter.record
}

// Error returns the error if the iteration is failed.
func (iter *WriteBatchIterator) Error() error {
	return iter.err
}

func (iter *WriteBatchIterator) decodeSlice() []byte {
	l := int(iter.decodeVarint())
	if l > len(iter.data) {
		iter.err = io.ErrShortBuffer
	}
	if iter.err != nil {
		return []byte{}
	}
	ret := iter.data[:l]
	iter.data = iter.data[l:]
	return ret
}

func (iter *WriteBatchIterator) decodeRecType() WriteBatchRecordType {
	if len(iter.data) == 0 {
		iter.err = io.ErrShortBuffer
		return WriteBatchRecordTypeNotUsed
	}
	t := iter.data[0]
	iter.data = iter.data[1:]
	return WriteBatchRecordType(t)
}

func (iter *WriteBatchIterator) decodeVarint() uint64 {
	var n int
	var x uint64
	for shift := uint(0); shift < 64 && n < len(iter.data); shift += 7 {
		b := uint64(iter.data[n])
		n++
		x |= (b & 0x7F) << shift
		if (b & 0x80) == 0 {
			iter.data = iter.data[n:]
			return x
		}
	}
	if n == len(iter.data) {
		iter.err = io.ErrShortBuffer
	} else {
		iter.err = errors.New("malformed varint")
	}
	return 0
}
