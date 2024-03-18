package gorocksdb

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestOpenTransactionDb(t *testing.T) {
	db := newTestTransactionDB(t, "TestOpenTransactionDb", nil)
	defer db.Close()
}

func TestTransactionDbColumnFamilies(t *testing.T) {
	test_cf_names := []string{"default", "cf1", "cf2"}
	db, cf_handles := newTestTransactionDBColumnFamilies(t, "TestOpenTransactionDbColumnFamilies", test_cf_names)
	ensure.True(t, 3 == len(cf_handles))
	defer db.Close()

	cf_names, err := ListColumnFamilies(NewDefaultOptions(), db.name)
	ensure.Nil(t, err)
	ensure.True(t, 3 == len(cf_names))
	ensure.DeepEqual(t, cf_names, test_cf_names)

	for idx, cf_name := range test_cf_names {
		ensure.Nil(t, db.PutCF(NewDefaultWriteOptions(), cf_handles[idx], []byte(cf_name+"_key"), []byte(cf_name+"_value")))
	}

	for idx, cf_name := range test_cf_names {
		val, err := db.GetCF(NewDefaultReadOptions(), cf_handles[idx], []byte(cf_name+"_key"))
		ensure.Nil(t, err)
		ensure.DeepEqual(t, val.Data(), []byte(cf_name+"_value"))
	}

	// Delete all keys in all column families
	for idx, cf_name := range test_cf_names {
		ensure.Nil(t, db.DeleteCF(NewDefaultWriteOptions(), cf_handles[idx], []byte(cf_name+"_key")))
	}

	for idx, cf_name := range test_cf_names {
		val, err := db.GetCF(NewDefaultReadOptions(), cf_handles[idx], []byte(cf_name+"_key"))
		ensure.Nil(t, err)
		ensure.True(t, val.Size() == 0)
	}

	{
		cf_handle, err := db.CreateColumnFamily(NewDefaultOptions(), "new_cf")
		ensure.Nil(t, err)
		ensure.NotNil(t, cf_handle)
		cf_names, err := ListColumnFamilies(NewDefaultOptions(), db.name)
		ensure.Nil(t, err)
		ensure.True(t, 4 == len(cf_names))
	}
}

func TestTransactionDBCRUD(t *testing.T) {
	db := newTestTransactionDB(t, "TestTransactionDBGet", nil)
	defer db.Close()

	var (
		givenKey     = []byte("hello")
		givenVal1    = []byte("world1")
		givenVal2    = []byte("world2")
		givenTxnKey  = []byte("hello2")
		givenTxnKey2 = []byte("hello3")
		givenTxnVal1 = []byte("whatawonderful")
		wo           = NewDefaultWriteOptions()
		ro           = NewDefaultReadOptions()
		to           = NewDefaultTransactionOptions()
	)

	// create
	ensure.Nil(t, db.Put(wo, givenKey, givenVal1))

	// retrieve
	v1, err := db.Get(ro, givenKey)
	defer v1.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v1.Data(), givenVal1)

	// update
	ensure.Nil(t, db.Put(wo, givenKey, givenVal2))
	v2, err := db.Get(ro, givenKey)
	defer v2.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v2.Data(), givenVal2)

	// delete
	ensure.Nil(t, db.Delete(wo, givenKey))
	v3, err := db.Get(ro, givenKey)
	defer v3.Free()
	ensure.Nil(t, err)
	ensure.True(t, v3.Data() == nil)

	// transaction
	txn := db.TransactionBegin(wo, to, nil)
	defer txn.Destroy()
	// create
	ensure.Nil(t, txn.Put(givenTxnKey, givenTxnVal1))
	v4, err := txn.Get(ro, givenTxnKey)
	defer v4.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v4.Data(), givenTxnVal1)

	ensure.Nil(t, txn.Commit())
	v5, err := db.Get(ro, givenTxnKey)
	defer v5.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v5.Data(), givenTxnVal1)

	// transaction
	txn2 := db.TransactionBegin(wo, to, nil)
	defer txn2.Destroy()
	// create
	ensure.Nil(t, txn2.Put(givenTxnKey2, givenTxnVal1))
	// rollback
	ensure.Nil(t, txn2.Rollback())

	v6, err := txn2.Get(ro, givenTxnKey2)
	defer v6.Free()
	ensure.Nil(t, err)
	ensure.True(t, v6.Data() == nil)
	// transaction
	txn3 := db.TransactionBegin(wo, to, nil)
	defer txn3.Destroy()
	// delete
	ensure.Nil(t, txn3.Delete(givenTxnKey))
	ensure.Nil(t, txn3.Commit())

	v7, err := db.Get(ro, givenTxnKey)
	defer v7.Free()
	ensure.Nil(t, err)
	ensure.True(t, v7.Data() == nil)

}

func TestTransactionDBWriteBatchColumnFamilies(t *testing.T) {
	test_cf_names := []string{"default", "cf1", "cf2"}
	db, cf_handles := newTestTransactionDBColumnFamilies(t, "TestOpenTransactionDbColumnFamilies", test_cf_names)
	ensure.True(t, len(cf_handles) == 3)
	defer db.Close()

	var (
		wo = NewDefaultWriteOptions()
		ro = NewDefaultReadOptions()
	)

	// WriteBatch PutCF
	{
		batch := NewWriteBatch()
		for h_idx := 1; h_idx <= 2; h_idx++ {
			for k_idx := 0; k_idx <= 2; k_idx++ {
				batch.PutCF(cf_handles[h_idx], []byte(fmt.Sprintf("%s_key_%d", test_cf_names[h_idx], k_idx)),
					[]byte(fmt.Sprintf("%s_value_%d", test_cf_names[h_idx], k_idx)))
			}
		}
		ensure.Nil(t, db.Write(wo, batch))
		batch.Destroy()
	}

	// Read back
	{
		for h_idx := 1; h_idx <= 2; h_idx++ {
			for k_idx := 0; k_idx <= 2; k_idx++ {
				data, err := db.GetCF(ro, cf_handles[h_idx], []byte(fmt.Sprintf("%s_key_%d", test_cf_names[h_idx], k_idx)))
				ensure.Nil(t, err)
				ensure.DeepEqual(t, data.Data(), []byte(fmt.Sprintf("%s_value_%d", test_cf_names[h_idx], k_idx)))
			}
		}
	}

	{ // WriteBatch with DeleteRangeCF not implemented
		batch := NewWriteBatch()
		batch.DeleteRangeCF(cf_handles[1], []byte(test_cf_names[1]+"_key_0"), []byte(test_cf_names[1]+"_key_2"))
		ensure.NotNil(t, db.Write(wo, batch))
	}
	// WriteBatch DeleteCF
	{
		batch := NewWriteBatch()
		batch.DeleteCF(cf_handles[1], []byte(test_cf_names[1]+"_key_0"))
		batch.DeleteCF(cf_handles[1], []byte(test_cf_names[1]+"_key_1"))
		ensure.Nil(t, db.Write(wo, batch))
	}

	// Read back the remaining keys
	{
		// All keys on "cf2" are still there.
		// Only key2 on "cf1" still remains
		for h_idx := 1; h_idx <= 2; h_idx++ {
			for k_idx := 0; k_idx <= 2; k_idx++ {
				data, err := db.GetCF(ro, cf_handles[h_idx], []byte(fmt.Sprintf("%s_key_%d", test_cf_names[h_idx], k_idx)))
				ensure.Nil(t, err)
				if h_idx == 2 || k_idx == 2 {
					ensure.DeepEqual(t, data.Data(), []byte(fmt.Sprintf("%s_value_%d", test_cf_names[h_idx], k_idx)))
				} else {
					ensure.True(t, len(data.Data()) == 0)
				}
			}
		}
	}
}

func TestTransactionDBCRUDColumnFamilies(t *testing.T) {
	test_cf_names := []string{"default", "cf1", "cf2"}
	db, cf_handles := newTestTransactionDBColumnFamilies(t, "TestOpenTransactionDbColumnFamilies", test_cf_names)
	ensure.True(t, len(cf_handles) == 3)
	defer db.Close()

	var (
		wo = NewDefaultWriteOptions()
		ro = NewDefaultReadOptions()
		to = NewDefaultTransactionOptions()
	)

	{
		txn := db.TransactionBegin(wo, to, nil)
		defer txn.Destroy()
		// RYW.
		for idx, cf_handle := range cf_handles {
			ensure.Nil(t, txn.PutCF(cf_handle, []byte(test_cf_names[idx]+"_key"), []byte(test_cf_names[idx]+"_value")))
			val, err := txn.GetCF(ro, cf_handle, []byte(test_cf_names[idx]+"_key"))
			defer val.Free()
			ensure.Nil(t, err)
			ensure.DeepEqual(t, val.Data(), []byte(test_cf_names[idx]+"_value"))
		}
		txn.Commit()
	}

	// Read after commit
	for idx, cf_handle := range cf_handles {
		val, err := db.GetCF(ro, cf_handle, []byte(test_cf_names[idx]+"_key"))
		defer val.Free()
		ensure.Nil(t, err)
		ensure.DeepEqual(t, val.Data(), []byte(test_cf_names[idx]+"_value"))
	}

	// Delete
	{
		txn := db.TransactionBegin(wo, to, nil)
		defer txn.Destroy()
		// RYW.
		for idx, cf_handle := range cf_handles {
			ensure.Nil(t, txn.DeleteCF(cf_handle, []byte(test_cf_names[idx]+"_key")))
		}
		txn.Commit()
	}

	// Read after delete commit
	for idx, cf_handle := range cf_handles {
		val, err := db.GetCF(ro, cf_handle, []byte(test_cf_names[idx]+"_key"))
		defer val.Free()
		ensure.Nil(t, err)
		ensure.True(t, val.Size() == 0)
	}
}

func TestTransactionDBGetForUpdate(t *testing.T) {
	lockTimeoutMilliSec := int64(50)
	applyOpts := func(opts *Options, transactionDBOpts *TransactionDBOptions) {
		transactionDBOpts.SetTransactionLockTimeout(lockTimeoutMilliSec)
	}
	db := newTestTransactionDB(t, "TestOpenTransactionDb", applyOpts)
	defer db.Close()

	var (
		givenKey = []byte("hello")
		givenVal = []byte("world")
		wo       = NewDefaultWriteOptions()
		ro       = NewDefaultReadOptions()
		to       = NewDefaultTransactionOptions()
	)

	txn := db.TransactionBegin(wo, to, nil)
	defer txn.Destroy()

	v, err := txn.GetForUpdate(ro, givenKey)
	defer v.Free()
	ensure.Nil(t, err)

	// expect lock timeout error to be thrown
	if err := db.Put(wo, givenKey, givenVal); err == nil {
		t.Error("expect locktime out error, got nil error")
	}
}

func TestTransactionDBGetForUpdateColumnFamilies(t *testing.T) {
	test_cf_names := []string{"default", "cf1", "cf2"}
	db, cf_handles := newTestTransactionDBColumnFamilies(t, "TestOpenTransactionDbColumnFamilies", test_cf_names)
	ensure.True(t, 3 == len(cf_handles))
	defer db.Close()

	var (
		wo = NewDefaultWriteOptions()
		ro = NewDefaultReadOptions()
		to = NewDefaultTransactionOptions()
	)

	{
		txn := db.TransactionBegin(wo, to, nil)
		defer txn.Destroy()

		val, err := txn.GetForUpdateCF(ro, cf_handles[1], []byte(test_cf_names[1]+"_key"))
		defer val.Free()
		ensure.Nil(t, err)
		txn.PutCF(cf_handles[1], []byte(test_cf_names[1]+"_key"), []byte(test_cf_names[1]+"_value"))
		ensure.Nil(t, txn.Commit())
	}

	// Read after update
	val, err := db.GetCF(ro, cf_handles[1], []byte(test_cf_names[1]+"_key"))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, val.Data(), []byte(test_cf_names[1]+"_value"))
}

func TestTransactionDBGetPinnedForUpdateColumnFamilies(t *testing.T) {
	testCFNames := []string{"default", "cf1", "cf2"}
	db, cfHandles := newTestTransactionDBColumnFamilies(t, "TestOpenTransactionDbColumnFamilies", testCFNames)
	ensure.True(t, 3 == len(cfHandles))
	defer db.Close()

	var (
		wo = NewDefaultWriteOptions()
		ro = NewDefaultReadOptions()
		to = NewDefaultTransactionOptions()
	)

	{
		txn := db.TransactionBegin(wo, to, nil)
		defer txn.Destroy()

		val, err := txn.GetPinnedForUpdateCF(ro, cfHandles[1], []byte(testCFNames[1]+"_key"))
		defer val.Destroy()
		ensure.Nil(t, err)
		txn.PutCF(cfHandles[1], []byte(testCFNames[1]+"_key"), []byte(testCFNames[1]+"_value"))
		ensure.Nil(t, txn.Commit())
	}

	// Read after update
	val, err := db.GetCF(ro, cfHandles[1], []byte(testCFNames[1]+"_key"))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, val.Data(), []byte(testCFNames[1]+"_value"))
}

func newTestTransactionDB(t *testing.T, name string, applyOpts func(opts *Options, transactionDBOpts *TransactionDBOptions)) *TransactionDB {
	dir, err := ioutil.TempDir("", "gorockstransactiondb-"+name)
	ensure.Nil(t, err)

	opts := NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	transactionDBOpts := NewDefaultTransactionDBOptions()
	if applyOpts != nil {
		applyOpts(opts, transactionDBOpts)
	}
	db, err := OpenTransactionDb(opts, transactionDBOpts, dir)
	ensure.Nil(t, err)

	return db
}

func newTestTransactionDBColumnFamilies(t *testing.T, name string, cfNames []string) (*TransactionDB, []*ColumnFamilyHandle) {
	dir, err := ioutil.TempDir("", "gorockstransactiondb-"+name)
	ensure.Nil(t, err)

	opts := NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	transactionDBOpts := NewDefaultTransactionDBOptions()
	cfOpts := []*Options{opts, opts, opts}
	db, cfHandles, err := OpenTransactionDbColumnFamilies(opts, transactionDBOpts, dir, cfNames, cfOpts)
	ensure.Nil(t, err)
	ensure.True(t, 3 == len(cfHandles))

	return db, cfHandles
}
