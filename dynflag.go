//go:build !embed
// +build !embed

package gorocksdb

// #cgo LDFLAGS: -L/Users/aditya/code/Keevo/.rocksdb-repo -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd
// #cgo CFLAGS: -I/Users/aditya/code/Keevo/.rocksdb-repo/include
import "C"
