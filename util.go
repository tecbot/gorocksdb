package gorocksdb

import "C"

import (
	"reflect"
	"unsafe"
)

// Btoi converts a bool value to int
func Btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// BoolToChar converts a bool value to C.uchar
func BoolToChar(b bool) C.uchar {
	if b {
		return 1
	}
	return 0
}

// CharToBool converts a C.uchar value to bool
func CharToBool(c C.uchar) bool {
	if c == 0 {
		return false
	}
	return true
}

func CharToByte(data *C.char, len C.size_t) []byte {
	var value []byte

	sH := (*reflect.SliceHeader)(unsafe.Pointer(&value))
	sH.Cap, sH.Len, sH.Data = int(len), int(len), uintptr(unsafe.Pointer(data))

	return value
}

// ByteToChar returns *C.char from byte slice
func ByteToChar(b []byte) *C.char {
	var c *C.char
	if len(b) > 0 {
		c = (*C.char)(unsafe.Pointer(&b[0]))
	}

	return c
}

// StringToChar returns *C.char from string
func StringToChar(s string) *C.char {
	ptrStr := (*reflect.StringHeader)(unsafe.Pointer(&s))

	return (*C.char)(unsafe.Pointer(ptrStr.Data))
}
