// +build rocksdb_v6

package gorocksdb

import "C"

// SetDataBlockIndexType sets the index type that will be used for the data block.
func (opts *BlockBasedTableOptions) SetDataBlockIndexType(value DataBlockIndexType) {
	C.rocksdb_block_based_options_set_data_block_index_type(opts.c, C.int(value))
}

// SetDataBlockIndexType sets the hash radio that will be used for the data block.
// #entries/#buckets. It is valid only when data_block_hash_index_type is
// kDataBlockBinaryAndHash.
func (opts *BlockBasedTableOptions) SetDataBlockHashRadio(value float64) {
	C.rocksdb_block_based_options_set_data_block_hash_ratio(opts.c, C.double(value))
}
