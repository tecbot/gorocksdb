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
    // This function is deprecated and removed in RocksDB v10.2.1
    return NULL;
}

void gorocksdb_filterpolicy_delete_filter(void* state, const char* v, size_t s) {
    free((char*)v);
}

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

void gorocksdb_mergeoperator_delete_value(void* id, const char* v, size_t s) {
    free((char*)v);
}

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

#define DEFAULT_PAGE_ALLOC_SIZE 512

extern gorocksdb_many_keys_t* gorocksdb_iter_many_keys(rocksdb_iterator_t* iter, int limit, bool reverse, const gorocksdb_many_keys_filter_t* key_filter, int page_alloc_size) {
    int i;
    char** keys, **values;
    size_t* key_sizes, *value_sizes;
    size_t key_size, value_size, cmp_size;

    // todo: we malloc the prefetch size (improve it)
    gorocksdb_many_keys_t* many_keys = (gorocksdb_many_keys_t*) malloc(sizeof(gorocksdb_many_keys_t));

    int size = page_alloc_size;
    if (size <= 0) {
        size = DEFAULT_PAGE_ALLOC_SIZE;
    }
    if (limit > 0 && limit < size) {
        size = limit;
    }
    keys = (char**) malloc(size * sizeof(char*));
    key_sizes = (size_t*) malloc(size * sizeof(size_t));
    values = (char**) malloc(size * sizeof(char*));
    value_sizes = (size_t*) malloc(size * sizeof(size_t));

    i = 0;
    while (rocksdb_iter_valid(iter)) {
        // Get current key
        const char* key = rocksdb_iter_key(iter, &key_size);

        // Check filter
        if (key_filter->key_prefix_s > 0) {
            if (key_size < key_filter->key_prefix_s) {
                break;
            }
            if (memcmp(key_filter->key_prefix, key, key_filter->key_prefix_s) != 0) {
                break;
            }
        }
        if (key_filter->key_end_s > 0) {
            cmp_size = key_size > key_filter->key_end_s ? key_filter->key_end_s : key_size;
            int c = memcmp(key, key_filter->key_end, cmp_size);
            if (c == 0 && key_filter->key_end_s == key_size) {
                // keys are equals, we break
                break;
            }
            if (reverse) {
                if (c == 0 && key_filter->key_end_s > key_size) {
                    // key_end is bigger than key, we must stop
                    break;
                } else if (c < 0) {
                    // key is smaller than key_end, we break
                    break;
                }
            } else {
                if (c == 0 && key_size > key_filter->key_end_s) {
                    // key_end is smaller than key, we must stop
                    break;
                } else if (c > 0) {
                    // key is greater than key_end, we break
                    break;
                }
            }
        }

        // Store key
        if (i == size) {
            // realloc 2x existing size
            size = size*2;
            keys = (char**) realloc(keys, size * sizeof(char*));
            key_sizes = (size_t*) realloc(key_sizes, size * sizeof(size_t));
            values = (char**) realloc(values, size * sizeof(char*));
            value_sizes = (size_t*) realloc(value_sizes, size * sizeof(size_t));
        }
        keys[i] = (char*) malloc(key_size * sizeof(char));
        memcpy(keys[i], key, key_size);
        key_sizes[i] = key_size;

        // Get value and store it
        const char* val = rocksdb_iter_value(iter, &value_size);
        if (val != NULL) {
            values[i] = (char*) malloc(value_size * sizeof(char));
            memcpy(values[i], val, value_size);
        } else {
            values[i] = NULL;
        }
        value_sizes[i] = value_size;
        i++;

        // Next key
        if (reverse) {
            // Move prev
            rocksdb_iter_prev(iter);
        } else {
            // Move next
            rocksdb_iter_next(iter);
        }

        // Check limit
        if (limit > 0 && i == limit) {
            break;
        }
    }

    many_keys->keys = keys;
    many_keys->key_sizes = key_sizes;
    many_keys->values = values;
    many_keys->value_sizes = value_sizes;
    many_keys->found = i;
    return many_keys;
}

extern void gorocksdb_destroy_many_keys(gorocksdb_many_keys_t* many_keys) {
    int i;
    for (i = 0; i < many_keys->found; i++) {
        free(many_keys->keys[i]);
        if (many_keys->values != NULL && many_keys->values[i] != NULL) {
                free(many_keys->values[i]);
        }
    }
    free(many_keys->keys);
    free(many_keys->key_sizes);
    if (many_keys->values != NULL) {
        free(many_keys->values);
        free(many_keys->value_sizes);
    }
    free(many_keys);
}

void _seek(rocksdb_iterator_t* iter, char* to_key, size_t to_key_s, bool reverse, bool exclude_to_key) {
    // seek
    if (reverse) {
        if (to_key_s > 0) {
            rocksdb_iter_seek_for_prev(iter, to_key, to_key_s);
        } else {
            rocksdb_iter_seek_to_last(iter);
        }
    } else {
        if (to_key_s > 0) {
            rocksdb_iter_seek(iter, to_key, to_key_s);
        } else {
            rocksdb_iter_seek_to_first(iter);
        }
    }
    // jump current?
    if (exclude_to_key && rocksdb_iter_valid(iter)) {
        size_t key_size;
        const char* key = rocksdb_iter_key(iter, &key_size);
        if (to_key_s == key_size && memcmp(key, to_key, key_size) == 0) {
            if (reverse) {
                rocksdb_iter_prev(iter);
            } else {
                rocksdb_iter_next(iter);
            }
        }
    }
}

extern gorocksdb_many_keys_t** gorocksdb_many_search_keys(rocksdb_iterator_t* iter, const gorocksdb_keys_search_t* keys_searches, int size, int page_alloc_size) {
    int i;
    gorocksdb_many_keys_filter_t key_filter;
    gorocksdb_many_keys_t** result = (gorocksdb_many_keys_t**) malloc(size*sizeof(gorocksdb_many_keys_t*));
    for (i=0; i < size; i++) {
        _seek(iter, keys_searches[i].key_from, keys_searches[i].key_from_s, keys_searches[i].reverse, keys_searches[i].exclude_key_from);
    	key_filter.key_prefix = keys_searches[i].key_prefix;
    	key_filter.key_prefix_s = keys_searches[i].key_prefix_s;
    	key_filter.key_end = keys_searches[i].key_end;
    	key_filter.key_end_s = keys_searches[i].key_end_s;
    	result[i] = gorocksdb_iter_many_keys(iter, keys_searches[i].limit, keys_searches[i].reverse, &key_filter, page_alloc_size);
    }
    return result;
}

extern void gorocksdb_destroy_many_many_keys(gorocksdb_many_keys_t** many_many_keys, int size) {
    int i;
    for (i = 0; i < size; i++) {
        gorocksdb_destroy_many_keys(many_many_keys[i]);
    }
    free(many_many_keys);
}

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
) {
    int i;
    gorocksdb_many_keys_filter_t key_filter;
    gorocksdb_many_keys_t** result = (gorocksdb_many_keys_t**) malloc(size*sizeof(gorocksdb_many_keys_t*));
    for (i=0; i < size; i++) {
    	rocksdb_iter_seek(iter, key_froms[i], key_from_s[i]);
    	key_filter.key_prefix = key_prefixes[i];
    	key_filter.key_prefix_s = key_prefix_s[i];
    	key_filter.key_end = key_ends[i];
    	key_filter.key_end_s = key_end_s[i];
    	result[i] = gorocksdb_iter_many_keys(iter, limits[i], reverse[i], &key_filter, page_alloc_size);
    }
    return result;
}

void gorocksdb_writebatch_put_many(
    rocksdb_writebatch_t* batch,
    size_t num_pairs,
    char** keys,
    size_t* key_sizes,
    char** values,
    size_t* value_sizes
) {
    int i;
    for (i=0; i < num_pairs; i++) {
        rocksdb_writebatch_put(batch, keys[i], key_sizes[i], values[i], value_sizes[i]);
    }
}

void gorocksdb_writebatch_put_many_cf(
    rocksdb_writebatch_t* batch,
    rocksdb_column_family_handle_t* cf,
    size_t num_pairs,
    char** keys,
    size_t* key_sizes,
    char** values,
    size_t* value_sizes
) {
    int i;
    for (i=0; i < num_pairs; i++) {
        rocksdb_writebatch_put_cf(batch, cf, keys[i], key_sizes[i], values[i], value_sizes[i]);
    }
}
