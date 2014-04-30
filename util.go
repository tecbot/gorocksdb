package gorocksdb

import "C"

import (
	"reflect"
	"unsafe"
)

// btoi converts a bool value to int
func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// boolToChar converts a bool value to C.uchar
func boolToChar(b bool) C.uchar {
	if b {
		return 1
	}
	return 0
}

func charToByte(data *C.char, len C.size_t) []byte {
	var value []byte

	sH := (*reflect.SliceHeader)(unsafe.Pointer(&value))
	sH.Cap, sH.Len, sH.Data = int(len), int(len), uintptr(unsafe.Pointer(data))

	return value
}

// byteToChar returns *C.char from byte slice
func byteToChar(b []byte) *C.char {
	var c *C.char
	if len(b) > 0 {
		c = (*C.char)(unsafe.Pointer(&b[0]))
	}

	return c
}

// stringToChar returns *C.char from string
func stringToChar(s string) *C.char {
	ptrStr := (*reflect.StringHeader)(unsafe.Pointer(&s))

	return (*C.char)(unsafe.Pointer(ptrStr.Data))
}
