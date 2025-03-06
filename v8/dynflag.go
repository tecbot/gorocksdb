//go:build !linux || !rocksdbstatic
// +build !linux !rocksdbstatic

package gorocksdb

// #cgo LDFLAGS: -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd -ldl
import "C"
