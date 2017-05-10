package gorocksdb

// #include "rocksdb/c.h"
import "C"

// EnvOptions represents options for env.
type EnvOptions struct {
	c *C.rocksdb_envoptions_t
}

// NewDefaultEnvOptions creates a default EnvOptions object.
func NewDefaultEnvOptions() *EnvOptions {
	return NewNativeEnvOptions(C.rocksdb_envoptions_create())
}

// NewNativeEnvOptions creates a EnvOptions object.
func NewNativeEnvOptions(c *C.rocksdb_envoptions_t) *EnvOptions {
	return &EnvOptions{c: c}
}
