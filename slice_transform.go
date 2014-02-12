package gorocksdb

// #include "rocksdb/c.h"
// #include "gorocksdb.h"
import "C"

import "unsafe"

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

// A SliceTransform can be used as a prefix extractor.
type SliceTransform struct {
	c *C.rocksdb_slicetransform_t
}

// NewSliceTransform creates a new slice transform for the given handler.
func NewSliceTransform(handler SliceTransformHandler) *SliceTransform {
	return NewNativeSliceTransform(C.gorocksdb_slicetransform_create(unsafe.Pointer(&handler)))
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
}

//export gorocksdb_slicetransform_transform
func gorocksdb_slicetransform_transform(cHandler unsafe.Pointer, cKey *C.char, cKeyLen C.size_t, cDstLen *C.size_t) *C.char {
	key := CharToByte(cKey, cKeyLen)

	var handler SliceTransformHandler = *(*SliceTransformHandler)(cHandler)
	dst := handler.Transform(key)

	*cDstLen = C.size_t(len(dst))

	return ByteToChar(dst)
}

//export gorocksdb_slicetransform_in_domain
func gorocksdb_slicetransform_in_domain(cHandler unsafe.Pointer, cKey *C.char, cKeyLen C.size_t) C.uchar {
	key := CharToByte(cKey, cKeyLen)

	var handler SliceTransformHandler = *(*SliceTransformHandler)(cHandler)
	inDomain := handler.InDomain(key)

	return BoolToChar(inDomain)
}

//export gorocksdb_slicetransform_in_range
func gorocksdb_slicetransform_in_range(cHandler unsafe.Pointer, cKey *C.char, cKeyLen C.size_t) C.uchar {
	key := CharToByte(cKey, cKeyLen)

	var handler SliceTransformHandler = *(*SliceTransformHandler)(cHandler)
	inRange := handler.InRange(key)

	return BoolToChar(inRange)
}

//export gorocksdb_slicetransform_name
func gorocksdb_slicetransform_name(cHandler unsafe.Pointer) *C.char {
	var handler SliceTransformHandler = *(*SliceTransformHandler)(cHandler)

	return StringToChar(handler.Name())
}
