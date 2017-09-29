package gorocksdb

import (
	"fmt"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestIterator(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val")))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()
	var actualKeys [][]byte
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := make([]byte, 4)
		copy(key, iter.Key().Data())
		actualKeys = append(actualKeys, key)
	}
	ensure.Nil(t, iter.Err())
	ensure.DeepEqual(t, actualKeys, givenKeys)
}

func TestIteratorMany(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val")))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()
	var actualKeys [][]byte
	iter.SeekToFirst()

	manyKeys := iter.NextManyKeys(2)
	for manyKeys.Found() > 0 {
		fmt.Println(manyKeys.Found())
		for _, k := range manyKeys.Keys() {
			fmt.Println(string(k))
			newK := make([]byte, len(k))
			copy(newK, k)
			actualKeys = append(actualKeys, newK)
		}
		manyKeys.Destroy()
		manyKeys = iter.NextManyKeys(2)
	}
	manyKeys.Destroy()
	ensure.Nil(t, iter.Err())
	ensure.DeepEqual(t, actualKeys, givenKeys)
}
