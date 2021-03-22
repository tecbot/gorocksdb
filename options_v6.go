// +build rocksdb_v6

package gorocksdb

// SetAtomicFlush sets atomic_flush
// If true, RocksDB supports flushing multiple column families and committing
// their results atomically to MANIFEST. Note that it is not
// necessary to set atomic_flush to true if WAL is always enabled since WAL
// allows the database to be restored to the last persistent state in WAL.
// This option is useful when there are column families with writes NOT
// protected by WAL.
// For manual flush, application has to specify which column families to
// flush atomically in DB::Flush.
// For auto-triggered flush, RocksDB atomically flushes ALL column families.
//
// Currently, any WAL-enabled writes after atomic flush may be replayed
// independently if the process crashes later and tries to recover.
func (opts *Options) SetAtomicFlush(value bool) {
	C.rocksdb_options_set_atomic_flush(opts.c, boolToChar(value))
}

// SetBottommostCompression sets the compression algorithm
// that will be used for the bottommost level that contain files.
//
// Default: SnappyCompression, which gives lightweight but fast
// compression.
func (opts *Options) SetBottommostCompression(value CompressionType) {
	C.rocksdb_options_set_bottommost_compression(opts.c, C.int(value))
}

// SetCompressionOptions sets different options for compression algorithms.
// Default: nil
func (opts *Options) SetCompressionOptions(value *CompressionOptions) {
	C.rocksdb_options_set_compression_options(opts.c, C.int(value.WindowBits), C.int(value.Level), C.int(value.Strategy), C.int(value.MaxDictBytes))
	if value.ZstdMaxTrainBytes > 0 {
		C.rocksdb_options_set_compression_options_zstd_max_train_bytes(opts.c, C.int(value.ZstdMaxTrainBytes))
	}
}

// SetBottommostCompressionOptions sets different options for compression algorithms used by bottommost_compression
// if it is enabled. To enable it, please see the definition of
// CompressionOptions.
// Default: nil
func (opts *Options) SetBottommostCompressionOptions(value *CompressionOptions, enabled bool) {
	C.rocksdb_options_set_bottommost_compression_options(opts.c, C.int(value.WindowBits), C.int(value.Level), C.int(value.Strategy), C.int(value.MaxDictBytes), boolToChar(enabled))
	if value.ZstdMaxTrainBytes > 0 {
		C.rocksdb_options_set_bottommost_compression_options_zstd_max_train_bytes(opts.c, C.int(value.ZstdMaxTrainBytes), boolToChar(enabled))
	}
}

// SetMaxWriteBufferSizeToMaintain sets max_write_buffer_size_to_maintain
// The total maximum size(bytes) of write buffers to maintain in memory
// including copies of buffers that have already been flushed. This parameter
// only affects trimming of flushed buffers and does not affect flushing.
// This controls the maximum amount of write history that will be available
// in memory for conflict checking when Transactions are used. The actual
// size of write history (flushed Memtables) might be higher than this limit
// if further trimming will reduce write history total size below this
// limit. For example, if max_write_buffer_size_to_maintain is set to 64MB,
// and there are three flushed Memtables, with sizes of 32MB, 20MB, 20MB.
// Because trimming the next Memtable of size 20MB will reduce total memory
// usage to 52MB which is below the limit, RocksDB will stop trimming.
//
// When using an OptimisticTransactionDB:
// If this value is too low, some transactions may fail at commit time due
// to not being able to determine whether there were any write conflicts.
//
// When using a TransactionDB:
// If Transaction::SetSnapshot is used, TransactionDB will read either
// in-memory write buffers or SST files to do write-conflict checking.
// Increasing this value can reduce the number of reads to SST files
// done for conflict detection.
//
// Setting this value to 0 will cause write buffers to be freed immediately
// after they are flushed. If this value is set to -1,
// 'max_write_buffer_number * write_buffer_size' will be used.
//
// Default:
// If using a TransactionDB/OptimisticTransactionDB, the default value will
// be set to the value of 'max_write_buffer_number * write_buffer_size'
// if it is not explicitly set by the user.  Otherwise, the default is 0.
func (opts *Options) SetMaxWriteBufferSizeToMaintain(value int64) {
	C.rocksdb_options_set_max_write_buffer_size_to_maintain(opts.c, C.int64_t(value))
}

// SetRowCache sets a global cache for table-level rows.
// Default: nullptr (disabled)
// Not supported in ROCKSDB_LITE mode!
func (opts *Options) SetRowCache(cache *Cache) {
	C.rocksdb_options_set_row_cache(opts.c, cache.c)
}

// SetSkipCheckingSstFileSizesOnDbOpen sets skip_checking_sst_file_sizes_on_db_open
// If true, then DB::Open() will not fetch and check sizes of all sst files.
// This may significantly speed up startup if there are many sst files,
// especially when using non-default Env with expensive GetFileSize().
// We'll still check that all required sst files exist.
// If paranoid_checks is false, this option is ignored, and sst files are
// not checked at all.
//
// Default: false
func (opts *Options) SetSkipCheckingSstFileSizesOnDbOpen(value bool) {
	C.rocksdb_options_set_skip_checking_sst_file_sizes_on_db_open(opts.c, boolToChar(value))
}

// SetStatsPersistPeriodSec sets the stats persist period in seconds.
//
// if not zero, dump rocksdb.stats to RocksDB every stats_persist_period_sec
// Default: 600
func (opts *Options) SetStatsPersistPeriodSec(value int) {
	C.rocksdb_options_set_stats_persist_period_sec(opts.c, C.uint(value))
}

// SetUnorderedWrite enables unordered write
//
// Setting unordered_write to true trades higher write throughput with
// relaxing the immutability guarantee of snapshots. This violates the
// repeatability one expects from ::Get from a snapshot, as well as
// ::MultiGet and Iterator's consistent-point-in-time view property.
// If the application cannot tolerate the relaxed guarantees, it can implement
// its own mechanisms to work around that and yet benefit from the higher
// throughput. Using TransactionDB with WRITE_PREPARED write policy and
// two_write_queues=true is one way to achieve immutable snapshots despite
// unordered_write.
//
// By default, i.e., when it is false, rocksdb does not advance the sequence
// number for new snapshots unless all the writes with lower sequence numbers
// are already finished. This provides the immutability that we except from
// snapshots. Moreover, since Iterator and MultiGet internally depend on
// snapshots, the snapshot immutability results into Iterator and MultiGet
// offering consistent-point-in-time view. If set to true, although
// Read-Your-Own-Write property is still provided, the snapshot immutability
// property is relaxed: the writes issued after the snapshot is obtained (with
// larger sequence numbers) will be still not visible to the reads from that
// snapshot, however, there still might be pending writes (with lower sequence
// number) that will change the state visible to the snapshot after they are
// landed to the memtable.
//
// Default: false
func (opts *Options) SetUnorderedWrite(value bool) {
	C.rocksdb_options_set_unordered_write(opts.c, boolToChar(value))
}
