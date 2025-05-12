package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
// #include "gorocksdb.h"
import "C"
import (
	"bytes"
	"errors"
	"reflect"
	"unsafe"
)

// Iterator provides a way to seek to specific keys and iterate through
// the keyspace from that point, as well as access the values of those keys.
//
// For example:
//
//	     it := db.NewIterator(readOpts)
//	     defer it.Close()
//
//	     it.Seek([]byte("foo"))
//			for ; it.Valid(); it.Next() {
//	         fmt.Printf("Key: %v Value: %v\n", it.Key().Data(), it.Value().Data())
//			}
//
//	     if err := it.Err(); err != nil {
//	         return err
//	     }
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

var ManyKeysPageAllocSize int = 512

func (iter *Iterator) fetchNextManyKeys(reverse bool, limit int, keyPrefix, keyEnd []byte) *ManyKeys {
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
	return &ManyKeys{c: C.gorocksdb_iter_many_keys(iter.c, C.int(limit), C.bool(reverse), &cKeyFilter, C.int(ManyKeysPageAllocSize))}
}

// NextManyKeys...
func (iter *Iterator) NextManyKeys(limit int, keyPrefix, keyEnd []byte) *ManyKeys {
	return iter.fetchNextManyKeys(false, limit, keyPrefix, keyEnd)
}

// NextManyKeysF... (compat)
func (iter *Iterator) NextManyKeysF(limit int, keyPrefix, keyEnd []byte) *ManyKeys {
	return iter.NextManyKeys(limit, keyPrefix, keyEnd)
}

// PrevManyKeys...
func (iter *Iterator) PrevManyKeys(limit int, keyPrefix, keyEnd []byte) *ManyKeys {
	return iter.fetchNextManyKeys(true, limit, keyPrefix, keyEnd)
}

type KeysSearch struct {
	KeyFrom,
	KeyPrefix,
	KeyEnd []byte
	Limit          int
	Reverse        bool
	ExcludeKeyFrom bool
}

func (iter *Iterator) ManySearchKeys(searches []KeysSearch) *ManyManyKeys {
	nbSearches := len(searches)
	cManyKeysSearches := make([]C.gorocksdb_keys_search_t, nbSearches)
	for i := range searches {
		cKSearch := C.gorocksdb_keys_search_t{
			limit:            C.int(searches[i].Limit),
			reverse:          C.bool(searches[i].Reverse),
			exclude_key_from: C.bool(searches[i].ExcludeKeyFrom),
		}
		cKSearch.key_from = C.CString(string(searches[i].KeyFrom))
		cKSearch.key_from_s = C.size_t(len(searches[i].KeyFrom))
		if len(searches[i].KeyPrefix) > 0 {
			cKSearch.key_prefix = C.CString(string(searches[i].KeyPrefix))
			cKSearch.key_prefix_s = C.size_t(len(searches[i].KeyPrefix))
		}
		if len(searches[i].KeyEnd) > 0 {
			cKSearch.key_end = C.CString(string(searches[i].KeyEnd))
			cKSearch.key_end_s = C.size_t(len(searches[i].KeyEnd))
		}
		cManyKeysSearches[i] = cKSearch
	}
	cManyManyKeys := C.gorocksdb_many_search_keys(iter.c,
		(*C.gorocksdb_keys_search_t)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&cManyKeysSearches)).Data)),
		C.int(nbSearches),
		C.int(ManyKeysPageAllocSize),
	)
	// free
	for i := range searches {
		C.free(unsafe.Pointer(cManyKeysSearches[i].key_from))
		if len(searches[i].KeyPrefix) > 0 {
			C.free(unsafe.Pointer(cManyKeysSearches[i].key_prefix))
		}
		if len(searches[i].KeyEnd) > 0 {
			C.free(unsafe.Pointer(cManyKeysSearches[i].key_end))
		}
	}
	return &ManyManyKeys{c: cManyManyKeys, size: nbSearches}
}

//func (iter *Iterator) ManySearchKeysExp(searches []KeysSearch) *ManyManyKeys {
//	nbSearches := len(searches)
//
//	cKeyFroms := C.malloc(C.size_t(nbSearches) * C.size_t(unsafe.Sizeof(uintptr(0))))
//	defer C.free(cKeyFroms)
//	cKeyPrefixes := C.malloc(C.size_t(nbSearches) * C.size_t(unsafe.Sizeof(uintptr(0))))
//	defer C.free(cKeyPrefixes)
//	cKeyEnds := C.malloc(C.size_t(nbSearches) * C.size_t(unsafe.Sizeof(uintptr(0))))
//	defer C.free(cKeyEnds)
//	cKeyFromSizes := make([]C.size_t, nbSearches)
//	cKeyPrefixSizes := make([]C.size_t, nbSearches)
//	cKeyEndSizes := make([]C.size_t, nbSearches)
//	cLimits := make([]C.int, nbSearches)
//
//	for i := uintptr(0); i < uintptr(nbSearches); i++ {
//		search := searches[i]
//		*(**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(cKeyFroms)) + i*unsafe.Sizeof(cKeyFroms))) = byteToChar(search.KeyFrom)
//		*(**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(cKeyPrefixes)) + i*unsafe.Sizeof(cKeyPrefixes))) = byteToChar(search.KeyPrefix)
//		*(**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(cKeyEnds)) + i*unsafe.Sizeof(cKeyEnds))) = byteToChar(search.KeyEnd)
//		cKeyFromSizes[i] = C.size_t(len(searches[i].KeyFrom))
//		cKeyPrefixSizes[i] = C.size_t(len(searches[i].KeyPrefix))
//		cKeyEndSizes[i] = C.size_t(len(searches[i].KeyEnd))
//		cLimits[i] = C.int(searches[i].Limit)
//	}
//	cManyManyKeys := C.gorocksdb_many_search_keys_raw(
//		iter.c,
//		(**C.char)(unsafe.Pointer(cKeyFroms)),
//		(*C.size_t)(unsafe.Pointer(&cKeyFromSizes[0])),
//		(**C.char)(unsafe.Pointer(cKeyPrefixes)),
//		(*C.size_t)(unsafe.Pointer(&cKeyPrefixSizes[0])),
//		(**C.char)(unsafe.Pointer(cKeyEnds)),
//		(*C.size_t)(unsafe.Pointer(&cKeyEndSizes[0])),
//		(*C.int)(unsafe.Pointer(&cLimits[0])),
//		C.int(nbSearches),
//		C.int(ManyKeysPageAllocSize),
//	)
//	return &ManyManyKeys{c: cManyManyKeys, size: nbSearches}
//}

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

func (m *ManyKeys) Values() [][]byte {
	found := m.Found()
	values := make([][]byte, found)

	for i := uintptr(0); i < uintptr(found); i++ {
		chars := *(**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(m.c.values)) + i*unsafe.Sizeof(m.c.values)))
		size := *(*C.size_t)(unsafe.Pointer(uintptr(unsafe.Pointer(m.c.value_sizes)) + i*unsafe.Sizeof(m.c.value_sizes)))
		values[i] = charToByte(chars, size)
	}
	return values
}

func (m *ManyKeys) Each(each func(i int, key []byte, value []byte) bool) bool {
	found := m.Found()
	for i := uintptr(0); i < uintptr(found); i++ {
		chars := *(**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(m.c.keys)) + i*unsafe.Sizeof(m.c.keys)))
		size := *(*C.size_t)(unsafe.Pointer(uintptr(unsafe.Pointer(m.c.key_sizes)) + i*unsafe.Sizeof(m.c.key_sizes)))
		key := charToByte(chars, size)

		chars = *(**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(m.c.values)) + i*unsafe.Sizeof(m.c.values)))
		size = *(*C.size_t)(unsafe.Pointer(uintptr(unsafe.Pointer(m.c.value_sizes)) + i*unsafe.Sizeof(m.c.value_sizes)))
		value := charToByte(chars, size)

		if !each(int(i), key, value) {
			return false
		}
	}
	return true
}

type ManyManyKeys struct {
	c    **C.gorocksdb_many_keys_t
	size int
}

func (m ManyManyKeys) Result() []*ManyKeys {
	result := make([]*ManyKeys, m.size)
	for i := uintptr(0); i < uintptr(m.size); i++ {
		manyKeys := *(**C.gorocksdb_many_keys_t)(unsafe.Pointer(uintptr(unsafe.Pointer(m.c)) + i*unsafe.Sizeof(m.c)))
		result[i] = &ManyKeys{c: manyKeys}
	}
	return result
}

func (m ManyManyKeys) Destroy() {
	C.gorocksdb_destroy_many_many_keys(m.c, C.int(m.size))
}
