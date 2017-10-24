package gorocksdb

/*
On Linux, we build Rocks manually and statically link in Snappy
but our ancient GLIBC requires linking to librt
On OSX, we use Homebrew's pre-built RockDB static binary
but need to dynlink to Snappy
*/

// #cgo LDFLAGS: -lrocksdb -lstdc++ -lm
// #cgo linux LDFLAGS: -lrt
// #cgo darwin LDFLAGS: -lsnappy
import "C"
