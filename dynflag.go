// +build !linux !static

package rocks

// #cgo LDFLAGS: -lrocksdb -lstdc++ -lm -ldl
import "C"
