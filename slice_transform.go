package gorocksdb

// #include "rocksdb/c.h"
// #include "gorocksdb.h"
import "C"
import (
	"unsafe"
)

// A SliceTransform can be used as a prefix extractor.
type SliceTransform struct {
	c *C.rocksdb_slicetransform_t
}

type SliceTransformHandler interface {
	// Transform a src in domain to a dst in the range.
	Transform(src []byte) []byte

	// Determine whether this is a valid src upon the function applies.
	InDomain(src []byte) bool

	// Determine whether dst=Transform(src) for some src.
	InRange(src []byte) bool

	// Return the name of this transformation.
	Name() string
}

// NewSliceTransform creates a new slice transform for the given handler.
func NewSliceTransform(handler SliceTransformHandler) *SliceTransform {
	h := unsafe.Pointer(&handler)
	return NewNativeSliceTransform(C.gorocksdb_slicetransform_create(h))
}

// NewFixedPrefixTransform creates a new fixed prefix transform.
func NewFixedPrefixTransform(prefixLen int) *SliceTransform {
	return NewNativeSliceTransform(C.rocksdb_slicetransform_create_fixed_prefix(C.size_t(prefixLen)))
}

// NewNativeSliceTransform allocates a SliceTransform object.
func NewNativeSliceTransform(c *C.rocksdb_slicetransform_t) *SliceTransform {
	return &SliceTransform{c}
}

// Destroy deallocates the SliceTransform object.
func (self *SliceTransform) Destroy() {
	C.rocksdb_slicetransform_destroy(self.c)
	self.c = nil
}

//export gorocksdb_slicetransform_transform
func gorocksdb_slicetransform_transform(handler *SliceTransformHandler, cKey *C.char, cKeyLen C.size_t, cDstLen *C.size_t) *C.char {
	key := charToByte(cKey, cKeyLen)

	dst := (*handler).Transform(key)

	*cDstLen = C.size_t(len(dst))

	return byteToChar(dst)
}

//export gorocksdb_slicetransform_in_domain
func gorocksdb_slicetransform_in_domain(handler *SliceTransformHandler, cKey *C.char, cKeyLen C.size_t) C.uchar {
	key := charToByte(cKey, cKeyLen)

	inDomain := (*handler).InDomain(key)

	return boolToChar(inDomain)
}

//export gorocksdb_slicetransform_in_range
func gorocksdb_slicetransform_in_range(handler *SliceTransformHandler, cKey *C.char, cKeyLen C.size_t) C.uchar {
	key := charToByte(cKey, cKeyLen)

	inRange := (*handler).InRange(key)

	return boolToChar(inRange)
}

//export gorocksdb_slicetransform_name
func gorocksdb_slicetransform_name(handler *SliceTransformHandler) *C.char {
	return stringToChar((*handler).Name())
}
