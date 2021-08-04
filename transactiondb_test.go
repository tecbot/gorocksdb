package gorocksdb

import (
	"io/ioutil"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestOpenTransactionDb(t *testing.T) {
	db := newTestTransactionDB(t, "TestOpenTransactionDb", nil)
	defer db.Close()
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

func TestOpenOptimisticTransactionDb(t *testing.T) {
	db := newTestOptimisticTransactionDB(t, "TestOpenOptimisticTransactionDb", nil)
	defer db.Close()
}

func TestOptimisticTransactionDBCRUD(t *testing.T) {
	txdb := newTestOptimisticTransactionDB(t, "TestOptimisticTransactionDBGet", nil)
	defer txdb.Close()

	db := txdb.GetBaseDB()
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
		to           = NewDefaultOptimisticTransactionOptions()
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
	txn := txdb.TransactionBegin(wo, to, nil)
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
	txn2 := txdb.TransactionBegin(wo, to, nil)
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
	txn3 := txdb.TransactionBegin(wo, to, nil)
	defer txn3.Destroy()
	// delete
	ensure.Nil(t, txn3.Delete(givenTxnKey))
	ensure.Nil(t, txn3.Commit())

	v7, err := db.Get(ro, givenTxnKey)
	defer v7.Free()
	ensure.Nil(t, err)
	ensure.True(t, v7.Data() == nil)

}

func newTestOptimisticTransactionDB(t *testing.T, name string, applyOpts func(opts *Options)) *OptimisticTransactionDB {
	dir, err := ioutil.TempDir("", "gorockstransactiondb-"+name)
	ensure.Nil(t, err)

	opts := NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := OpenOptimisticTransactionDb(opts, dir)
	ensure.Nil(t, err)

	return db
}
