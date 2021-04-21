package gorocksdb

import (
	"io/ioutil"
	"sync"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestOpenOptimisticTransactionDb(t *testing.T) {
	db := newTestOptimisticTransactionDB(t, "TestOpenTransactionDb")
	defer db.Close()
}

func TestOptimisticTransactionDBCRUD(t *testing.T) {
	db := newTestOptimisticTransactionDB(t, "TestTransactionDbCRUD")
	defer db.Close()

	var (
		givenTxnKey  = []byte("hello2")
		givenTxnKey2 = []byte("hello3")
		givenTxnVal1 = []byte("whatawonderful")
		wo           = NewDefaultWriteOptions()
		ro           = NewDefaultReadOptions()
		to           = NewDefaultOptimisticTransactionOptions()
	)

	bdb := db.GetBaseDb()

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

	v5, err := bdb.Get(ro, givenTxnKey)
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

	v6, err := bdb.Get(ro, givenTxnKey2)
	defer v6.Free()
	ensure.Nil(t, err)
	ensure.True(t, v6.Data() == nil)

	// transaction
	txn3 := db.TransactionBegin(wo, to, nil)
	defer txn3.Destroy()
	// delete
	ensure.Nil(t, txn3.Delete(givenTxnKey))
	ensure.Nil(t, txn3.Commit())

	v7, err := bdb.Get(ro, givenTxnKey)
	defer v7.Free()
	ensure.Nil(t, err)
	ensure.True(t, v7.Data() == nil)
}

func TestOptimisticTransactionDBConflicts(t *testing.T) {
	db := newTestOptimisticTransactionDB(t, "TestOptimisticConflicts")
	defer db.Close()

	var (
		ctrKey = []byte("num")
		wo     = NewDefaultWriteOptions()
		ro     = NewDefaultReadOptions()
		to     = NewDefaultOptimisticTransactionOptions()
	)

	bdb := db.GetBaseDb()
	ensure.Nil(t, bdb.Put(wo, ctrKey, []byte{0}))
	targetCnt := 10

	var wg sync.WaitGroup
	for i := 1; i <= targetCnt; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				txn := db.TransactionBegin(wo, to, nil)
				cnt, err := txn.GetForUpdate(ro, ctrKey)
				ensure.Nil(t, err)
				val := cnt.Data()[0]
				newVal := val + 1
				ensure.Nil(t, txn.Put(ctrKey, []byte{newVal}))
				err = txn.Commit()
				cnt.Free()
				txn.Destroy()
				if err == nil {
					break
				}
			}
		}()
	}
	wg.Wait()
	cnt, err := bdb.Get(ro, ctrKey)
	defer cnt.Free()
	ensure.Nil(t, err)
	val := cnt.Data()[0]
	ensure.True(t, val == byte(targetCnt))
}

func newTestOptimisticTransactionDB(t *testing.T, name string) *OptimisticTransactionDB {
	dir, err := ioutil.TempDir("", "gorocksoptimistictransactiondb-"+name)
	ensure.Nil(t, err)

	opts := NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := OpenOptimisticTransactionDb(opts, dir)
	ensure.Nil(t, err)

	return db
}
