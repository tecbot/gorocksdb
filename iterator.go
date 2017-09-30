package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
// #include "gorocksdb.h"
import "C"
import (
	"bytes"
	"errors"
	"unsafe"
)

// Iterator provides a way to seek to specific keys and iterate through
// the keyspace from that point, as well as access the values of those keys.
//
// For example:
//
//      it := db.NewIterator(readOpts)
//      defer it.Close()
//
//      it.Seek([]byte("foo"))
//		for ; it.Valid(); it.Next() {
//          fmt.Printf("Key: %v Value: %v\n", it.Key().Data(), it.Value().Data())
// 		}
//
//      if err := it.Err(); err != nil {
//          return err
//      }
//
type Iterator struct {
	c *C.rocksdb_iterator_t
}

// NewNativeIterator creates a Iterator object.
func NewNativeIterator(c unsafe.Pointer) *Iterator {
	return &Iterator{(*C.rocksdb_iterator_t)(c)}
}

// Valid returns false only when an Iterator has iterated past either the
// first or the last key in the database.
func (iter *Iterator) Valid() bool {
	return C.rocksdb_iter_valid(iter.c) != 0
}

// ValidForPrefix returns false only when an Iterator has iterated past the
// first or the last key in the database or the specified prefix.
func (iter *Iterator) ValidForPrefix(prefix []byte) bool {
	if C.rocksdb_iter_valid(iter.c) == 0 {
		return false
	}

	key := iter.Key()
	result := bytes.HasPrefix(key.Data(), prefix)
	key.Free()
	return result
}

// Key returns the key the iterator currently holds.
func (iter *Iterator) Key() *Slice {
	var cLen C.size_t
	cKey := C.rocksdb_iter_key(iter.c, &cLen)
	if cKey == nil {
		return nil
	}
	return &Slice{cKey, cLen, true}
}

// Value returns the value in the database the iterator currently holds.
func (iter *Iterator) Value() *Slice {
	var cLen C.size_t
	cVal := C.rocksdb_iter_value(iter.c, &cLen)
	if cVal == nil {
		return nil
	}
	return &Slice{cVal, cLen, true}
}

// Next moves the iterator to the next sequential key in the database.
func (iter *Iterator) Next() {
	C.rocksdb_iter_next(iter.c)
}

type ManyKeys struct {
	c *C.gorocksdb_many_keys_t
}

func (m *ManyKeys) Destroy() {
	C.gorocksdb_destroy_many_keys(m.c)
}

func (m *ManyKeys) Found() int {
	return int(m.c.found)
}

func (m *ManyKeys) Keys() [][]byte {
	found := m.Found()
	keys := make([][]byte, found)

	for i := uintptr(0); i < uintptr(found); i++ {
		chars := *(**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(m.c.keys)) + i*unsafe.Sizeof(m.c.keys)))
		size := *(*C.size_t)(unsafe.Pointer(uintptr(unsafe.Pointer(m.c.key_sizes)) + i*unsafe.Sizeof(m.c.key_sizes)))
		keys[i] = charToByte(chars, size)

	}
	return keys
}

//....
func (iter *Iterator) NextManyKeys(size int) *ManyKeys {
	return &ManyKeys{c: C.gorocksdb_iter_next_many_keys(iter.c, C.int(size))}
}

//....
func (iter *Iterator) NextManyKeysF(size int, keyPrefix, keyEnd []byte) *ManyKeys {
	cKeyFilter := C.gorocksdb_many_keys_filter_t{}
	if len(keyPrefix) > 0 {
		cKeyPrefix := C.CString(string(keyPrefix))
		defer C.free(unsafe.Pointer(cKeyPrefix))
		cKeyFilter.key_prefix = cKeyPrefix
		cKeyFilter.key_prefix_s = C.size_t(len(keyPrefix))
	}
	if len(keyEnd) > 0 {
		cKeyEnd := C.CString(string(keyEnd))
		defer C.free(unsafe.Pointer(cKeyEnd))
		cKeyFilter.key_end = cKeyEnd
		cKeyFilter.key_end_s = C.size_t(len(keyEnd))
	}
	return &ManyKeys{c: C.gorocksdb_iter_next_many_keys_f(iter.c, C.int(size), &cKeyFilter)}
}

// Prev moves the iterator to the previous sequential key in the database.
func (iter *Iterator) Prev() {
	C.rocksdb_iter_prev(iter.c)
}

// SeekToFirst moves the iterator to the first key in the database.
func (iter *Iterator) SeekToFirst() {
	C.rocksdb_iter_seek_to_first(iter.c)
}

// SeekToLast moves the iterator to the last key in the database.
func (iter *Iterator) SeekToLast() {
	C.rocksdb_iter_seek_to_last(iter.c)
}

// Seek moves the iterator to the position greater than or equal to the key.
func (iter *Iterator) Seek(key []byte) {
	cKey := byteToChar(key)
	C.rocksdb_iter_seek(iter.c, cKey, C.size_t(len(key)))
}

// SeekForPrev moves the iterator to the last key that less than or equal
// to the target key, in contrast with Seek.
func (iter *Iterator) SeekForPrev(key []byte) {
	cKey := byteToChar(key)
	C.rocksdb_iter_seek_for_prev(iter.c, cKey, C.size_t(len(key)))
}

// Err returns nil if no errors happened during iteration, or the actual
// error otherwise.
func (iter *Iterator) Err() error {
	var cErr *C.char
	C.rocksdb_iter_get_error(iter.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Close closes the iterator.
func (iter *Iterator) Close() {
	C.rocksdb_iter_destroy(iter.c)
	iter.c = nil
}
