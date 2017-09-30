#include <stdlib.h>
#include "rocksdb/c.h"

typedef struct {
    char** keys;
    size_t* key_sizes;
    int found;

} gorocksdb_many_keys_t;


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

extern gorocksdb_many_keys_t* gorocksdb_iter_next_many_keys(rocksdb_iterator_t* iter, int size);
extern gorocksdb_many_keys_t* gorocksdb_iter_next_many_keys_f(rocksdb_iterator_t* iter, int size, const char* key_prefix, const char* key_limit);

extern void gorocksdb_destroy_many_keys(gorocksdb_many_keys_t* many_keys);
