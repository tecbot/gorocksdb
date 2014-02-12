package gorocksdb

// #include "rocksdb/c.h"
import "C"

// WriteBatch is a batching of Puts, Merges and Deletes.
// TODO: WriteBatchIterator
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

// Put queues a key-value pair.
func (self *WriteBatch) Put(key, value []byte) {
	cKey := ByteToChar(key)
	cValue := ByteToChar(value)

	C.rocksdb_writebatch_put(self.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// Merge queues a merge of "value" with the existing value of "key".
func (self *WriteBatch) Merge(key, value []byte) {
	cKey := ByteToChar(key)
	cValue := ByteToChar(value)

	C.rocksdb_writebatch_merge(self.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// Delete queues a deletion of the data at key.
func (self *WriteBatch) Delete(key []byte) {
	cKey := ByteToChar(key)

	C.rocksdb_writebatch_delete(self.c, cKey, C.size_t(len(key)))
}

// Clear removes all the enqueued Put and Deletes.
func (self *WriteBatch) Clear() {
	C.rocksdb_writebatch_clear(self.c)
}

// Destroy deallocates the WriteBatch object.
func (self *WriteBatch) Destroy() {
	C.rocksdb_writebatch_destroy(self.c)
	self.c = nil
}
