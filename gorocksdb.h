#include <stdlib.h>
#include <stdbool.h>
#include "rocksdb/c.h"

typedef struct {
    char** keys;
    size_t* key_sizes;
    char** values;
    size_t* value_sizes;
    int found;

} gorocksdb_many_keys_t;

// Compression types
#define rocksdb_no_compression 0
#define rocksdb_snappy_compression 1
#define rocksdb_zlib_compression 2
#define rocksdb_bz2_compression 3
#define rocksdb_lz4_compression 4
#define rocksdb_lz4hc_compression 5
#define rocksdb_xpress_compression 6
#define rocksdb_zstd_compression 7

#define FALSE false
#define TRUE true

// This API provides convenient C wrapper functions for rocksdb client.

/* Base */

extern void gorocksdb_destruct_handler(void* state);

/* CompactionFilter */

extern rocksdb_compactionfilter_t* gorocksdb_compactionfilter_create(uintptr_t idx);

/* Comparator */

extern rocksdb_comparator_t* gorocksdb_comparator_create(uintptr_t idx);

/* Filter Policy */

extern rocksdb_filterpolicy_t* gorocksdb_filterpolicy_create(uintptr_t idx);
extern void gorocksdb_filterpolicy_delete_filter(void* state, const char* v, size_t s);

/* Merge Operator */

extern rocksdb_mergeoperator_t* gorocksdb_mergeoperator_create(uintptr_t idx);
extern void gorocksdb_mergeoperator_delete_value(void* state, const char* v, size_t s);

/* Slice Transform */

extern rocksdb_slicetransform_t* gorocksdb_slicetransform_create(uintptr_t idx);

/* Iterate many keys */

typedef struct {
    char* key_prefix;
    size_t key_prefix_s;
    char* key_end;
    size_t key_end_s;

} gorocksdb_many_keys_filter_t;

extern gorocksdb_many_keys_t* gorocksdb_iter_many_keys(rocksdb_iterator_t* iter, int limit, bool reverse, const gorocksdb_many_keys_filter_t* key_filter, int page_alloc_size);

extern void gorocksdb_destroy_many_keys(gorocksdb_many_keys_t* many_keys);

/* Batch searches */

typedef struct {
    char* key_from;
    size_t key_from_s;
    char* key_prefix;
    size_t key_prefix_s;
    char* key_end;
    size_t key_end_s;
    int limit;
    bool reverse;
    bool exclude_key_from;

} gorocksdb_keys_search_t;

extern gorocksdb_many_keys_t** gorocksdb_many_search_keys(
    rocksdb_iterator_t* iter,
    const gorocksdb_keys_search_t* keys_searches,
    int size,
    int page_alloc_size
);

extern gorocksdb_many_keys_t** gorocksdb_many_search_keys_raw(
    rocksdb_iterator_t* iter,
    char** key_froms,
    size_t* key_from_s,
    char** key_prefixes,
    size_t* key_prefix_s,
    char** key_ends,
    size_t* key_end_s,
    int* limits,
    bool* reverse,
    int size,
    int page_alloc_size
);

extern void gorocksdb_destroy_many_many_keys(gorocksdb_many_keys_t** many_many_keys, int size);

/* Batch PutMany */

void gorocksdb_writebatch_put_many( 
    rocksdb_writebatch_t* batch,
    size_t num_pairs, 
    char** keys,
    size_t* key_sizes, 
    char** values, 
    size_t* value_sizes
);

void gorocksdb_writebatch_put_many_cf( 
    rocksdb_writebatch_t* batch,
    rocksdb_column_family_handle_t* cf, 
    size_t num_pairs, 
    char** keys,
    size_t* key_sizes, 
    char** values, 
    size_t* value_sizes
);
