#include "gorocksdb.h"
#include "_cgo_export.h"
#include <string.h>

/* Base */

void gorocksdb_destruct_handler(void* state) { }

/* Comparator */

rocksdb_comparator_t* gorocksdb_comparator_create(uintptr_t idx) {
    return rocksdb_comparator_create(
        (void*)idx,
        gorocksdb_destruct_handler,
        (int (*)(void*, const char*, size_t, const char*, size_t))(gorocksdb_comparator_compare),
        (const char *(*)(void*))(gorocksdb_comparator_name));
}

/* CompactionFilter */

rocksdb_compactionfilter_t* gorocksdb_compactionfilter_create(uintptr_t idx) {
    return rocksdb_compactionfilter_create(
        (void*)idx,
        gorocksdb_destruct_handler,
        (unsigned char (*)(void*, int, const char*, size_t, const char*, size_t, char**, size_t*, unsigned char*))(gorocksdb_compactionfilter_filter),
        (const char *(*)(void*))(gorocksdb_compactionfilter_name));
}

/* Filter Policy */

rocksdb_filterpolicy_t* gorocksdb_filterpolicy_create(uintptr_t idx) {
    return rocksdb_filterpolicy_create(
        (void*)idx,
        gorocksdb_destruct_handler,
        (char* (*)(void*, const char* const*, const size_t*, int, size_t*))(gorocksdb_filterpolicy_create_filter),
        (unsigned char (*)(void*, const char*, size_t, const char*, size_t))(gorocksdb_filterpolicy_key_may_match),
        gorocksdb_filterpolicy_delete_filter,
        (const char *(*)(void*))(gorocksdb_filterpolicy_name));
}

void gorocksdb_filterpolicy_delete_filter(void* state, const char* v, size_t s) { }

/* Merge Operator */

rocksdb_mergeoperator_t* gorocksdb_mergeoperator_create(uintptr_t idx) {
    return rocksdb_mergeoperator_create(
        (void*)idx,
        gorocksdb_destruct_handler,
        (char* (*)(void*, const char*, size_t, const char*, size_t, const char* const*, const size_t*, int, unsigned char*, size_t*))(gorocksdb_mergeoperator_full_merge),
        (char* (*)(void*, const char*, size_t, const char* const*, const size_t*, int, unsigned char*, size_t*))(gorocksdb_mergeoperator_partial_merge_multi),
        gorocksdb_mergeoperator_delete_value,
        (const char* (*)(void*))(gorocksdb_mergeoperator_name));
}

void gorocksdb_mergeoperator_delete_value(void* id, const char* v, size_t s) { }

/* Slice Transform */

rocksdb_slicetransform_t* gorocksdb_slicetransform_create(uintptr_t idx) {
    return rocksdb_slicetransform_create(
    	(void*)idx,
    	gorocksdb_destruct_handler,
    	(char* (*)(void*, const char*, size_t, size_t*))(gorocksdb_slicetransform_transform),
    	(unsigned char (*)(void*, const char*, size_t))(gorocksdb_slicetransform_in_domain),
    	(unsigned char (*)(void*, const char*, size_t))(gorocksdb_slicetransform_in_range),
    	(const char* (*)(void*))(gorocksdb_slicetransform_name));
}

gorocksdb_many_keys_t* gorocksdb_iter_next_many_keys(rocksdb_iterator_t* iter, int size) {
    int i = 0;
    gorocksdb_many_keys_t* many_keys = (gorocksdb_many_keys_t*) malloc(sizeof(gorocksdb_many_keys_t));

    char** keys;
    size_t* key_sizes;
    keys = (char**) malloc(size * sizeof(char*));
    key_sizes = (size_t*) malloc(size * sizeof(size_t));

    for (i = 0; i < size; i++) {
        if (!rocksdb_iter_valid(iter)) {
            break;
        }

        // Stuff
        const char* key = rocksdb_iter_key(iter, &key_sizes[i]);
        keys[i] = (char*) malloc(key_sizes[i] * sizeof(char));
        memcpy(keys[i], key, key_sizes[i]);

        rocksdb_iter_next(iter);
    }

    many_keys->keys = keys;
    many_keys->key_sizes = key_sizes;
    many_keys->found = i;
    return many_keys;
}

void gorocksdb_destroy_many_keys(gorocksdb_many_keys_t* many_keys) {
    for (int i = 0; i < many_keys->found; i++) {
        free(many_keys->keys[i]);
    }

    free(many_keys->keys);
    free(many_keys->key_sizes);
    free(many_keys);
}
