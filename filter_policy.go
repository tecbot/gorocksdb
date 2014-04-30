package gorocksdb

// #include "rocksdb/c.h"
// #include "gorocksdb.h"
import "C"
import (
	"unsafe"
)

// FilterPolicy is a factory type that allows the RocksDB database to create a
// filter, such as a bloom filter, which will used to reduce reads.
type FilterPolicy struct {
	c *C.rocksdb_filterpolicy_t
}

type FilterPolicyHandler interface {
	// keys contains a list of keys (potentially with duplicates)
	// that are ordered according to the user supplied comparator.
	CreateFilter(keys [][]byte) []byte

	// "filter" contains the data appended by a preceding call to
	// CreateFilter(). This method must return true if
	// the key was in the list of keys passed to CreateFilter().
	// This method may return true or false if the key was not on the
	// list, but it should aim to return false with a high probability.
	KeyMayMatch(key []byte, filter []byte) bool

	// Return the name of this policy.
	Name() string
}

// NewFilterPolicy creates a new filter policy for the given handler.
func NewFilterPolicy(handler FilterPolicyHandler) *FilterPolicy {
	h := unsafe.Pointer(&handler)
	return NewNativeFilterPolicy(C.gorocksdb_filterpolicy_create(h))
}

// Return a new filter policy that uses a bloom filter with approximately
// the specified number of bits per key.  A good value for bits_per_key
// is 10, which yields a filter with ~1% false positive rate.
//
// Note: if you are using a custom comparator that ignores some parts
// of the keys being compared, you must not use NewBloomFilterPolicy()
// and must provide your own FilterPolicy that also ignores the
// corresponding parts of the keys.  For example, if the comparator
// ignores trailing spaces, it would be incorrect to use a
// FilterPolicy (like NewBloomFilterPolicy) that does not ignore
// trailing spaces in keys.
func NewBloomFilter(bitsPerKey int) *FilterPolicy {
	return NewNativeFilterPolicy(C.rocksdb_filterpolicy_create_bloom(C.int(bitsPerKey)))
}

// NewNativeFilterPolicy creates a filter policy object.
func NewNativeFilterPolicy(c *C.rocksdb_filterpolicy_t) *FilterPolicy {
	return &FilterPolicy{c}
}

// Destroy deallocates the FilterPolicy object.
func (self *FilterPolicy) Destroy() {
	C.rocksdb_filterpolicy_destroy(self.c)
	self.c = nil
}

//export gorocksdb_filterpolicy_create_filter
func gorocksdb_filterpolicy_create_filter(handler *FilterPolicyHandler, cKeys **C.char, cKeysLen *C.size_t, cNumKeys C.int, cDstLen *C.size_t) *C.char {
	keys := make([][]byte, int(cNumKeys))
	for i, l := 0, int(cNumKeys); i < l; i++ {
		cKey := C.gorocksdb_get_char_at_index(cKeys, C.int(i))
		cKeyLen := C.gorocksdb_get_int_at_index(cKeysLen, C.int(i))

		keys[i] = CharToByte(cKey, cKeyLen)
	}

	dst := (*handler).CreateFilter(keys)

	*cDstLen = C.size_t(len(dst))

	return ByteToChar(dst)
}

//export gorocksdb_filterpolicy_key_may_match
func gorocksdb_filterpolicy_key_may_match(handler *FilterPolicyHandler, cKey *C.char, cKeyLen C.size_t, cFilter *C.char, cFilterLen C.size_t) C.uchar {
	key := CharToByte(cKey, cKeyLen)
	filter := CharToByte(cFilter, cFilterLen)

	match := (*handler).KeyMayMatch(key, filter)

	return BoolToChar(match)
}

//export gorocksdb_filterpolicy_name
func gorocksdb_filterpolicy_name(handler *FilterPolicyHandler) *C.char {
	return StringToChar((*handler).Name())
}
