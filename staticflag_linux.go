// +build static

package rocks

// #cgo LDFLAGS: -l:librocksdb.a -l:libstdc++.a -lm -ldl
import "C"
