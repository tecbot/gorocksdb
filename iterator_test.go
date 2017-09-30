package gorocksdb

import (
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
		for _, k := range manyKeys.Keys() {
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

func TestIteratorManyFOnKeyPrefix(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("keyA1"), []byte("keyA2"), []byte("keyA3"), []byte("keyB1")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val")))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()
	var actualKeys [][]byte
	iter.SeekToFirst()

	manyKeys := iter.NextManyKeysF(2, []byte("keyA"), nil)
	for manyKeys.Found() > 0 {
		for _, k := range manyKeys.Keys() {
			newK := make([]byte, len(k))
			copy(newK, k)
			actualKeys = append(actualKeys, newK)
		}
		manyKeys.Destroy()
		manyKeys = iter.NextManyKeysF(2, []byte("keyA"), nil)
	}
	manyKeys.Destroy()
	ensure.Nil(t, iter.Err())
	ensure.DeepEqual(t, actualKeys, [][]byte{[]byte("keyA1"), []byte("keyA2"), []byte("keyA3")})
}

func TestIteratorManyFOnKeyEnd(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("A"), []byte("B"), []byte("C"), []byte("C1"), []byte("D")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val")))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()
	var actualKeys [][]byte
	iter.SeekToFirst()

	manyKeys := iter.NextManyKeysF(2, nil, []byte("C1"))
	for manyKeys.Found() > 0 {
		for _, k := range manyKeys.Keys() {
			newK := make([]byte, len(k))
			copy(newK, k)
			actualKeys = append(actualKeys, newK)
		}
		manyKeys.Destroy()
		manyKeys = iter.NextManyKeysF(2, nil, []byte("C1"))
	}
	manyKeys.Destroy()
	ensure.Nil(t, iter.Err())
	ensure.DeepEqual(t, actualKeys, [][]byte{[]byte("A"), []byte("B"), []byte("C")})
}

func TestIteratorManyFOnKeyPrefixAndEnd(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("keyA"), []byte("keyB"), []byte("keyC"), []byte("keyC1")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val")))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()
	var actualKeys [][]byte
	iter.SeekToFirst()

	manyKeys := iter.NextManyKeysF(2, []byte("key"), []byte("keyC1"))
	for manyKeys.Found() > 0 {
		for _, k := range manyKeys.Keys() {
			newK := make([]byte, len(k))
			copy(newK, k)
			actualKeys = append(actualKeys, newK)
		}
		manyKeys.Destroy()
		manyKeys = iter.NextManyKeysF(2, []byte("key"), []byte("keyC1"))
	}
	manyKeys.Destroy()
	ensure.Nil(t, iter.Err())
	ensure.DeepEqual(t, actualKeys, [][]byte{[]byte("keyA"), []byte("keyB"), []byte("keyC")})
}
