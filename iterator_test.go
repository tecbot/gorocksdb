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

func TestIteratorNextManyWithKeyPrefix(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("keyA1"), []byte("keyA2"), []byte("keyA3"), []byte("keyB1")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val_"+string(k))))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()
	var actualKeys [][]byte
	var actualValues [][]byte
	iter.SeekToFirst()

	manyKeys := iter.NextManyKeys(2, []byte("keyA"), nil)
	for manyKeys.Found() > 0 {
		for _, k := range manyKeys.Keys() {
			newK := make([]byte, len(k))
			copy(newK, k)
			actualKeys = append(actualKeys, newK)
		}
		for _, v := range manyKeys.Values() {
			newV := make([]byte, len(v))
			copy(newV, v)
			actualValues = append(actualValues, newV)
		}
		manyKeys.Destroy()
		manyKeys = iter.NextManyKeys(2, []byte("keyA"), nil)
	}
	manyKeys.Destroy()
	ensure.Nil(t, iter.Err())
	ensure.DeepEqual(t, actualKeys, [][]byte{[]byte("keyA1"), []byte("keyA2"), []byte("keyA3")})
	ensure.DeepEqual(t, actualValues, [][]byte{[]byte("val_keyA1"), []byte("val_keyA2"), []byte("val_keyA3")})
}

func TestIteratorNextManyWithLimit(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("keyA1"), []byte("keyA2"), []byte("keyA3"), []byte("keyA4")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val_"+string(k))))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()

	iter.SeekToFirst()
	manyKeys := iter.NextManyKeys(-1, []byte("keyA"), nil)
	ensure.DeepEqual(t, manyKeys.Keys(), [][]byte{[]byte("keyA1"), []byte("keyA2"), []byte("keyA3"), []byte("keyA4")})
	ensure.DeepEqual(t, manyKeys.Values(), [][]byte{[]byte("val_keyA1"), []byte("val_keyA2"), []byte("val_keyA3"), []byte("val_keyA4")})
	manyKeys.Destroy()

	iter.SeekToFirst()
	manyKeys = iter.NextManyKeys(0, []byte("keyA"), nil)
	ensure.DeepEqual(t, manyKeys.Keys(), [][]byte{[]byte("keyA1"), []byte("keyA2"), []byte("keyA3"), []byte("keyA4")})
	ensure.DeepEqual(t, manyKeys.Values(), [][]byte{[]byte("val_keyA1"), []byte("val_keyA2"), []byte("val_keyA3"), []byte("val_keyA4")})
	manyKeys.Destroy()

	iter.SeekToFirst()
	manyKeys = iter.NextManyKeys(2, []byte("keyA"), nil)
	ensure.DeepEqual(t, manyKeys.Keys(), [][]byte{[]byte("keyA1"), []byte("keyA2")})
	ensure.DeepEqual(t, manyKeys.Values(), [][]byte{[]byte("val_keyA1"), []byte("val_keyA2")})
	manyKeys.Destroy()

	iter.SeekToFirst()
	manyKeys = iter.NextManyKeys(20, []byte("keyA"), nil)
	ensure.DeepEqual(t, manyKeys.Keys(), [][]byte{[]byte("keyA1"), []byte("keyA2"), []byte("keyA3"), []byte("keyA4")})
	ensure.DeepEqual(t, manyKeys.Values(), [][]byte{[]byte("val_keyA1"), []byte("val_keyA2"), []byte("val_keyA3"), []byte("val_keyA4")})
	manyKeys.Destroy()
}

func TestIteratorNextManyWithKeyEnd(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("A"), []byte("B"), []byte("C"), []byte("C10"), []byte("C11"), []byte("D")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val_"+string(k))))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()
	var actualKeys [][]byte
	var actualValues [][]byte
	iter.SeekToFirst()

	manyKeys := iter.NextManyKeys(2, nil, []byte("C1"))
	for manyKeys.Found() > 0 {
		for _, k := range manyKeys.Keys() {
			newK := make([]byte, len(k))
			copy(newK, k)
			actualKeys = append(actualKeys, newK)
		}
		for _, v := range manyKeys.Values() {
			newV := make([]byte, len(v))
			copy(newV, v)
			actualValues = append(actualValues, newV)
		}
		manyKeys.Destroy()
		manyKeys = iter.NextManyKeys(2, nil, []byte("C1"))
	}
	manyKeys.Destroy()
	ensure.Nil(t, iter.Err())
	ensure.DeepEqual(t, actualKeys, [][]byte{[]byte("A"), []byte("B"), []byte("C")})
	ensure.DeepEqual(t, actualValues, [][]byte{[]byte("val_A"), []byte("val_B"), []byte("val_C")})
}

func TestIteratorNextManyWithKeyPrefixAndEnd(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("keyA"), []byte("keyB"), []byte("keyC"), []byte("keyC1")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val_"+string(k))))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()
	var actualKeys [][]byte
	var actualValues [][]byte
	iter.SeekToFirst()

	manyKeys := iter.NextManyKeys(2, []byte("key"), []byte("keyC1"))
	for manyKeys.Found() > 0 {
		for _, k := range manyKeys.Keys() {
			newK := make([]byte, len(k))
			copy(newK, k)
			actualKeys = append(actualKeys, newK)
		}
		for _, v := range manyKeys.Values() {
			newV := make([]byte, len(v))
			copy(newV, v)
			actualValues = append(actualValues, newV)
		}
		manyKeys.Destroy()
		manyKeys = iter.NextManyKeys(2, []byte("key"), []byte("keyC1"))
	}
	manyKeys.Destroy()
	ensure.Nil(t, iter.Err())
	ensure.DeepEqual(t, actualKeys, [][]byte{[]byte("keyA"), []byte("keyB"), []byte("keyC")})
	ensure.DeepEqual(t, actualValues, [][]byte{[]byte("val_keyA"), []byte("val_keyB"), []byte("val_keyC")})
}

func TestIteratorPrevManyWithKeyPrefix(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("keyA1"), []byte("keyA2"), []byte("keyA3"), []byte("keyB1")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val_"+string(k))))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()
	var actualKeys [][]byte
	var actualValues [][]byte

	iter.SeekToLast()
	manyKeys := iter.PrevManyKeys(2, []byte("keyA"), nil)
	ensure.DeepEqual(t, manyKeys.Found(), 0)

	iter.Seek([]byte("keyA3"))
	manyKeys = iter.PrevManyKeys(2, []byte("keyA"), nil)
	for manyKeys.Found() > 0 {
		for _, k := range manyKeys.Keys() {
			newK := make([]byte, len(k))
			copy(newK, k)
			actualKeys = append(actualKeys, newK)
		}
		for _, v := range manyKeys.Values() {
			newV := make([]byte, len(v))
			copy(newV, v)
			actualValues = append(actualValues, newV)
		}
		manyKeys.Destroy()
		manyKeys = iter.PrevManyKeys(2, []byte("keyA"), nil)
	}
	manyKeys.Destroy()
	ensure.Nil(t, iter.Err())
	ensure.DeepEqual(t, actualKeys, [][]byte{[]byte("keyA3"), []byte("keyA2"), []byte("keyA1")})
	ensure.DeepEqual(t, actualValues, [][]byte{[]byte("val_keyA3"), []byte("val_keyA2"), []byte("val_keyA1")})
}

func TestIteratorPrevManyWithLimit(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("keyA1"), []byte("keyA2"), []byte("keyA3"), []byte("keyA4")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val_"+string(k))))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()

	iter.SeekToLast()
	manyKeys := iter.PrevManyKeys(-1, []byte("keyA"), nil)
	ensure.DeepEqual(t, manyKeys.Keys(), [][]byte{[]byte("keyA4"), []byte("keyA3"), []byte("keyA2"), []byte("keyA1")})
	ensure.DeepEqual(t, manyKeys.Values(), [][]byte{[]byte("val_keyA4"), []byte("val_keyA3"), []byte("val_keyA2"), []byte("val_keyA1")})
	manyKeys.Destroy()

	iter.SeekToLast()
	manyKeys = iter.PrevManyKeys(0, []byte("keyA"), nil)
	ensure.DeepEqual(t, manyKeys.Keys(), [][]byte{[]byte("keyA4"), []byte("keyA3"), []byte("keyA2"), []byte("keyA1")})
	ensure.DeepEqual(t, manyKeys.Values(), [][]byte{[]byte("val_keyA4"), []byte("val_keyA3"), []byte("val_keyA2"), []byte("val_keyA1")})
	manyKeys.Destroy()

	iter.SeekToLast()
	manyKeys = iter.PrevManyKeys(2, []byte("keyA"), nil)
	ensure.DeepEqual(t, manyKeys.Keys(), [][]byte{[]byte("keyA4"), []byte("keyA3")})
	ensure.DeepEqual(t, manyKeys.Values(), [][]byte{[]byte("val_keyA4"), []byte("val_keyA3")})
	manyKeys.Destroy()

	iter.SeekToLast()
	manyKeys = iter.PrevManyKeys(20, []byte("keyA"), nil)
	ensure.DeepEqual(t, manyKeys.Keys(), [][]byte{[]byte("keyA4"), []byte("keyA3"), []byte("keyA2"), []byte("keyA1")})
	ensure.DeepEqual(t, manyKeys.Values(), [][]byte{[]byte("val_keyA4"), []byte("val_keyA3"), []byte("val_keyA2"), []byte("val_keyA1")})
	manyKeys.Destroy()
}

func TestIteratorPrevManyWithKeyEnd(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("A"), []byte("B"), []byte("C"), []byte("C11"), []byte("C12"), []byte("D")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val_"+string(k))))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()
	var actualKeys [][]byte
	var actualValues [][]byte
	iter.SeekToLast()

	manyKeys := iter.PrevManyKeys(2, nil, []byte("C1"))
	for manyKeys.Found() > 0 {
		for _, k := range manyKeys.Keys() {
			newK := make([]byte, len(k))
			copy(newK, k)
			actualKeys = append(actualKeys, newK)
		}
		for _, v := range manyKeys.Values() {
			newV := make([]byte, len(v))
			copy(newV, v)
			actualValues = append(actualValues, newV)
		}
		manyKeys.Destroy()
		manyKeys = iter.PrevManyKeys(2, nil, []byte("C1"))
	}
	manyKeys.Destroy()
	ensure.Nil(t, iter.Err())
	ensure.DeepEqual(t, actualKeys, [][]byte{[]byte("D"), []byte("C12"), []byte("C11")})
	ensure.DeepEqual(t, actualValues, [][]byte{[]byte("val_D"), []byte("val_C12"), []byte("val_C11")})
}

func TestIteratorPrevManyWithKeyPrefixAndEnd(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("keyA"), []byte("keyB"), []byte("keyC"), []byte("keyC1")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val_"+string(k))))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()
	var actualKeys [][]byte
	var actualValues [][]byte
	iter.SeekToLast()

	manyKeys := iter.PrevManyKeys(2, []byte("key"), []byte("keyA"))
	for manyKeys.Found() > 0 {
		for _, k := range manyKeys.Keys() {
			newK := make([]byte, len(k))
			copy(newK, k)
			actualKeys = append(actualKeys, newK)
		}
		for _, v := range manyKeys.Values() {
			newV := make([]byte, len(v))
			copy(newV, v)
			actualValues = append(actualValues, newV)
		}
		manyKeys.Destroy()
		manyKeys = iter.PrevManyKeys(2, []byte("key"), []byte("keyA"))
	}
	manyKeys.Destroy()
	ensure.Nil(t, iter.Err())
	ensure.DeepEqual(t, actualKeys, [][]byte{[]byte("keyC1"), []byte("keyC"), []byte("keyB")})
	ensure.DeepEqual(t, actualValues, [][]byte{[]byte("val_keyC1"), []byte("val_keyC"), []byte("val_keyB")})
}

func TestIteratorManySearchKeys(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("A"), []byte("B"), []byte("C"), []byte("D"), []byte("E"), []byte("F")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val_"+string(k))))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()

	searches := make([]KeysSearch, 3)
	searches[0] = KeysSearch{KeyFrom: []byte("A"), Limit: 1000}
	searches[1] = KeysSearch{KeyFrom: []byte("D"), Limit: 1000}
	searches[2] = KeysSearch{KeyFrom: []byte("Z"), Limit: 1000}

	manyManyKeys := iter.ManySearchKeys(searches)
	defer manyManyKeys.Destroy()
	result := manyManyKeys.Result()
	if len(result) != len(searches) {
		t.Fatalf("result len should be %d", len(searches))
	}
	ensure.DeepEqual(t, result[0].Found(), 6)
	ensure.DeepEqual(t, result[0].Keys(), [][]byte{[]byte("A"), []byte("B"), []byte("C"), []byte("D"), []byte("E"), []byte("F")})
	ensure.DeepEqual(t, result[0].Values(), [][]byte{[]byte("val_A"), []byte("val_B"), []byte("val_C"), []byte("val_D"), []byte("val_E"), []byte("val_F")})
	ensure.DeepEqual(t, result[1].Found(), 3)
	ensure.DeepEqual(t, result[1].Keys(), [][]byte{[]byte("D"), []byte("E"), []byte("F")})
	ensure.DeepEqual(t, result[1].Values(), [][]byte{[]byte("val_D"), []byte("val_E"), []byte("val_F")})
	ensure.DeepEqual(t, result[2].Found(), 0)
	ensure.DeepEqual(t, result[2].Keys(), [][]byte{})
	ensure.DeepEqual(t, result[2].Values(), [][]byte{})
}

func TestIteratorManySearchKeysEmptyKeyFrom(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("A"), []byte("B"), []byte("C"), []byte("D"), []byte("E"), []byte("F")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val_"+string(k))))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()

	searches := make([]KeysSearch, 4)
	searches[0] = KeysSearch{Limit: 3}
	searches[1] = KeysSearch{Limit: 3, ExcludeKeyFrom: true}
	searches[2] = KeysSearch{Limit: 3, Reverse: true}
	searches[3] = KeysSearch{Limit: 3, ExcludeKeyFrom: true, Reverse: true}

	manyManyKeys := iter.ManySearchKeys(searches)
	defer manyManyKeys.Destroy()
	result := manyManyKeys.Result()
	if len(result) != len(searches) {
		t.Fatalf("result len should be %d", len(searches))
	}
	ensure.DeepEqual(t, result[0].Found(), 3)
	ensure.DeepEqual(t, result[0].Keys(), [][]byte{[]byte("A"), []byte("B"), []byte("C")})
	ensure.DeepEqual(t, result[0].Values(), [][]byte{[]byte("val_A"), []byte("val_B"), []byte("val_C")})
	ensure.DeepEqual(t, result[0].Keys(), result[1].Keys())
	ensure.DeepEqual(t, result[0].Values(), result[1].Values())
	ensure.DeepEqual(t, result[2].Found(), 3)
	ensure.DeepEqual(t, result[2].Keys(), [][]byte{[]byte("F"), []byte("E"), []byte("D")})
	ensure.DeepEqual(t, result[2].Values(), [][]byte{[]byte("val_F"), []byte("val_E"), []byte("val_D")})
	ensure.DeepEqual(t, result[2].Keys(), result[3].Keys())
	ensure.DeepEqual(t, result[2].Values(), result[3].Values())
}

func TestIteratorManySearchKeysReverse(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("A1"), []byte("A2"), []byte("C1"), []byte("C2"), []byte("D"), []byte("E"), []byte("F")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val_"+string(k))))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()

	searches := make([]KeysSearch, 2)
	searches[0] = KeysSearch{KeyFrom: []byte("C3"), Limit: 1000, Reverse: true}
	searches[1] = KeysSearch{KeyFrom: []byte("C2"), Limit: 1000, Reverse: true}

	manyManyKeys := iter.ManySearchKeys(searches)
	defer manyManyKeys.Destroy()
	result := manyManyKeys.Result()
	if len(result) != len(searches) {
		t.Fatalf("result len should be %d", len(searches))
	}
	ensure.DeepEqual(t, result[0].Found(), 4)
	ensure.DeepEqual(t, result[0].Keys(), [][]byte{[]byte("C2"), []byte("C1"), []byte("A2"), []byte("A1")})
	ensure.DeepEqual(t, result[0].Values(), [][]byte{[]byte("val_C2"), []byte("val_C1"), []byte("val_A2"), []byte("val_A1")})
	ensure.DeepEqual(t, result[1].Found(), 4)
	ensure.DeepEqual(t, result[1].Keys(), [][]byte{[]byte("C2"), []byte("C1"), []byte("A2"), []byte("A1")})
	ensure.DeepEqual(t, result[1].Values(), [][]byte{[]byte("val_C2"), []byte("val_C1"), []byte("val_A2"), []byte("val_A1")})
}

func TestIteratorManySearchKeysExcludeKeyFrom(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("A"), []byte("B"), []byte("C"), []byte("D"), []byte("E"), []byte("F")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val_"+string(k))))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()

	searches := make([]KeysSearch, 5)
	searches[0] = KeysSearch{KeyFrom: []byte("A"), Limit: 1000, Reverse: false, ExcludeKeyFrom: true}
	searches[1] = KeysSearch{KeyFrom: []byte("D"), Limit: 1000, Reverse: false, ExcludeKeyFrom: false}
	searches[2] = KeysSearch{KeyFrom: []byte("A"), Limit: 1000, Reverse: true, ExcludeKeyFrom: true}
	searches[3] = KeysSearch{KeyFrom: []byte("A"), Limit: 1000, Reverse: true, ExcludeKeyFrom: false}
	searches[4] = KeysSearch{KeyFrom: []byte("D"), Limit: 1000, Reverse: true, ExcludeKeyFrom: false}
	manyManyKeys := iter.ManySearchKeys(searches)
	defer manyManyKeys.Destroy()
	result := manyManyKeys.Result()
	if len(result) != len(searches) {
		t.Fatalf("result len should be %d", len(searches))
	}
	ensure.DeepEqual(t, result[0].Found(), 5)
	ensure.DeepEqual(t, result[0].Keys(), [][]byte{[]byte("B"), []byte("C"), []byte("D"), []byte("E"), []byte("F")})
	ensure.DeepEqual(t, result[0].Values(), [][]byte{[]byte("val_B"), []byte("val_C"), []byte("val_D"), []byte("val_E"), []byte("val_F")})
	ensure.DeepEqual(t, result[1].Found(), 3)
	ensure.DeepEqual(t, result[1].Keys(), [][]byte{[]byte("D"), []byte("E"), []byte("F")})
	ensure.DeepEqual(t, result[1].Values(), [][]byte{[]byte("val_D"), []byte("val_E"), []byte("val_F")})
	ensure.DeepEqual(t, result[2].Found(), 0)
	ensure.DeepEqual(t, result[2].Keys(), [][]byte{})
	ensure.DeepEqual(t, result[2].Values(), [][]byte{})
	ensure.DeepEqual(t, result[3].Found(), 1)
	ensure.DeepEqual(t, result[3].Keys(), [][]byte{[]byte("A")})
	ensure.DeepEqual(t, result[3].Values(), [][]byte{[]byte("val_A")})
	ensure.DeepEqual(t, result[4].Found(), 4)
	ensure.DeepEqual(t, result[4].Keys(), [][]byte{[]byte("D"), []byte("C"), []byte("B"), []byte("A")})
	ensure.DeepEqual(t, result[4].Values(), [][]byte{[]byte("val_D"), []byte("val_C"), []byte("val_B"), []byte("val_A")})
}

func TestIteratorManySearchKeysWithKeyPrefix(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("A1"), []byte("A2"), []byte("B1"), []byte("C1"), []byte("D1"), []byte("D2")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val_"+string(k))))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()

	searches := make([]KeysSearch, 4)
	searches[0] = KeysSearch{KeyFrom: []byte("A"), KeyPrefix: []byte("A"), Limit: 1000}
	searches[1] = KeysSearch{KeyFrom: []byte("B"), KeyPrefix: []byte("B"), Limit: 1000}
	searches[2] = KeysSearch{KeyFrom: []byte("D"), KeyPrefix: []byte("D"), Limit: 1000}
	searches[3] = KeysSearch{KeyFrom: []byte("Z"), KeyPrefix: []byte("Z"), Limit: 1000}

	manyManyKeys := iter.ManySearchKeys(searches)
	defer manyManyKeys.Destroy()
	result := manyManyKeys.Result()
	if len(result) != len(searches) {
		t.Fatalf("result len should be %d", len(searches))
	}
	ensure.DeepEqual(t, result[0].Found(), 2)
	ensure.DeepEqual(t, result[0].Keys(), [][]byte{[]byte("A1"), []byte("A2")})
	ensure.DeepEqual(t, result[0].Values(), [][]byte{[]byte("val_A1"), []byte("val_A2")})
	ensure.DeepEqual(t, result[1].Found(), 1)
	ensure.DeepEqual(t, result[1].Keys(), [][]byte{[]byte("B1")})
	ensure.DeepEqual(t, result[1].Values(), [][]byte{[]byte("val_B1")})
	ensure.DeepEqual(t, result[2].Found(), 2)
	ensure.DeepEqual(t, result[2].Keys(), [][]byte{[]byte("D1"), []byte("D2")})
	ensure.DeepEqual(t, result[2].Values(), [][]byte{[]byte("val_D1"), []byte("val_D2")})
	ensure.DeepEqual(t, result[3].Found(), 0)
	ensure.DeepEqual(t, result[3].Keys(), [][]byte{})
	ensure.DeepEqual(t, result[3].Values(), [][]byte{})
}

func TestIteratorManySearchKeysWithKeyEnd(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("A1"), []byte("A2"), []byte("A3"), []byte("B1"), []byte("B2"), []byte("B3")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val_"+string(k))))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()

	searches := make([]KeysSearch, 2)
	searches[0] = KeysSearch{KeyFrom: []byte("A"), KeyEnd: []byte("A3"), Limit: 1000}
	searches[1] = KeysSearch{KeyFrom: []byte("B"), KeyEnd: []byte("B2"), Limit: 1000}

	manyManyKeys := iter.ManySearchKeys(searches)
	defer manyManyKeys.Destroy()
	result := manyManyKeys.Result()
	if len(result) != len(searches) {
		t.Fatalf("result len should be %d", len(searches))
	}
	ensure.DeepEqual(t, result[0].Found(), 2)
	ensure.DeepEqual(t, result[0].Keys(), [][]byte{[]byte("A1"), []byte("A2")})
	ensure.DeepEqual(t, result[0].Values(), [][]byte{[]byte("val_A1"), []byte("val_A2")})
	ensure.DeepEqual(t, result[1].Found(), 1)
	ensure.DeepEqual(t, result[1].Keys(), [][]byte{[]byte("B1")})
	ensure.DeepEqual(t, result[1].Values(), [][]byte{[]byte("val_B1")})
}

func TestIteratorManySearchKeysWithKeyPrefixAndEnd(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("keyC"), []byte("keyC0"), []byte("keyC1")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val_"+string(k))))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()

	searches := make([]KeysSearch, 2)
	searches[0] = KeysSearch{KeyFrom: []byte("keyC0"), KeyPrefix: []byte("keyC"), KeyEnd: []byte("keyC1"), Limit: 1000}
	searches[1] = KeysSearch{KeyFrom: []byte("k"), KeyPrefix: []byte("keyC"), KeyEnd: []byte("keyC1"), Limit: 1000}

	manyManyKeys := iter.ManySearchKeys(searches)
	defer manyManyKeys.Destroy()
	result := manyManyKeys.Result()
	if len(result) != len(searches) {
		t.Fatalf("result len should be %d", len(searches))
	}
	ensure.DeepEqual(t, result[0].Found(), 1)
	ensure.DeepEqual(t, result[0].Keys(), [][]byte{[]byte("keyC0")})
	ensure.DeepEqual(t, result[0].Values(), [][]byte{[]byte("val_keyC0")})
	ensure.DeepEqual(t, result[1].Found(), 2)
	ensure.DeepEqual(t, result[1].Keys(), [][]byte{[]byte("keyC"), []byte("keyC0")})
	ensure.DeepEqual(t, result[1].Values(), [][]byte{[]byte("val_keyC"), []byte("val_keyC0")})
}

func TestIteratorNextManyKeysEach(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("keyA1"), []byte("keyA2"), []byte("keyA3"), []byte("keyA4")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val_"+string(k))))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()

	iter.SeekToFirst()
	manyKeys := iter.NextManyKeys(-1, []byte("keyA"), nil)

	actualKeys := [][]byte{}
	actualValues := [][]byte{}
	all := manyKeys.Each(func(i int, key []byte, value []byte) bool {
		actualKeys = append(actualKeys, key)
		actualValues = append(actualValues, value)
		return true
	})
	ensure.DeepEqual(t, all, true)
	ensure.DeepEqual(t, actualKeys, [][]byte{[]byte("keyA1"), []byte("keyA2"), []byte("keyA3"), []byte("keyA4")})
	ensure.DeepEqual(t, actualValues, [][]byte{[]byte("val_keyA1"), []byte("val_keyA2"), []byte("val_keyA3"), []byte("val_keyA4")})

	actualKeys = nil
	actualValues = nil
	limit := 2
	all = manyKeys.Each(func(i int, key []byte, value []byte) bool {
		actualKeys = append(actualKeys, key)
		actualValues = append(actualValues, value)
		return len(actualKeys) != limit
	})
	ensure.DeepEqual(t, all, false)
	ensure.DeepEqual(t, actualKeys, [][]byte{[]byte("keyA1"), []byte("keyA2")})
	ensure.DeepEqual(t, actualValues, [][]byte{[]byte("val_keyA1"), []byte("val_keyA2")})

	manyKeys.Destroy()
}
