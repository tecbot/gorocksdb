package gorocksdb

// #include "rocksdb/c.h"
import "C"

type FilterPolicy struct {
	c *C.rocksdb_filterpolicy_t
}

// NewFilterPolicy creates a FilterPolicy object.
func NewFilterPolicy(c *C.rocksdb_filterpolicy_t) FilterPolicy {
	return FilterPolicy{c}
}

// NewBloomFilter returns a new filter policy that uses a bloom filter with approximately
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
func NewBloomFilter(bitsPerKey float64) FilterPolicy {
	return NewFilterPolicy(C.rocksdb_filterpolicy_create_bloom(C.double(bitsPerKey)))
}

// NewBloomFilterFull returns a new filter policy created with use_block_based_builder=false
// (use full or partitioned filter).
func NewBloomFilterFull(bitsPerKey float64) FilterPolicy {
	return NewFilterPolicy(C.rocksdb_filterpolicy_create_bloom_full(C.double(bitsPerKey)))
}

// NewRibbonFilter returns a new filter policy created with a ribbon filter.
func NewRibbonFilter(bitsPerKey float64) FilterPolicy {
	return NewFilterPolicy(C.rocksdb_filterpolicy_create_ribbon(C.double(bitsPerKey)))
}

// NewRibbonHybridFilter returns a new filter policy created with a ribbon hybrid filter.
func NewRibbonHybridFilter(bitsPerKey float64, bloomBeforeLevel int) FilterPolicy {
	return NewFilterPolicy(C.rocksdb_filterpolicy_create_ribbon_hybrid(C.double(bitsPerKey), C.int(bloomBeforeLevel)))
}
