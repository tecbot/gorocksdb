package gorocksdb

import (
	"testing"

	"github.com/facebookgo/ensure"
	"fmt"
	"time"
	"math/rand"

	"io/ioutil"
)

func SlowWriter (db *DB, count int, name string, cf *ColumnFamilyHandle) {
	wo := NewDefaultWriteOptions()
	for i:=0; i<count; i++ {
		key := fmt.Sprintf("%s%d", name, i)
		err := db.PutCF(wo, cf, []byte(key), []byte("value"))
		if err!=nil {
			fmt.Println("> WRITE ERROR", err.Error())
		} else {
			//fmt.Printf("> %d %s\n", i, key)
		}
		time.Sleep(time.Duration(rand.Int()%10)) // 0..9.99ms
		if (i+1)%100 == 0 {
			fmt.Printf("generated %d records\n", i+1)
			time.Sleep(time.Second)
		}
	}
	fmt.Println(">i think i am done", name)
}

func TestWalIterator(t *testing.T) {
	dir, err := ioutil.TempDir("", "gorocksdb-wal-cf")
	fmt.Println("DIR", dir)
	if err!=nil {
		t.Fail()
		t.Log(err.Error())
		return
	}
	var cf_names = []string{"default", "one", "two", "three"}

	opts := NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetWALTtlSeconds(1)

	var cfopts = []*Options{opts, opts, opts, opts}

	db, handles, err := OpenDbColumnFamilies(opts, dir, cf_names, cfopts)
	if err!=nil {
		t.Fail()
		t.Log(err.Error())
		return
	}
	//db := newTestDB(t, "TestWalIterator", nil)

	_ = handles

	//wo := NewDefaultWriteOptions()
	//db.Put(wo, []byte("start_key"), []byte("value"))
	count := 1<<10
	go SlowWriter(db, count>>2, "one", handles[1])
	go SlowWriter(db, count>>2, "two", handles[2])
	go SlowWriter(db, count>>2, "three", handles[3])
	go SlowWriter(db, count>>2, "default", handles[0])
	var i int
	var seq uint64
	var iter *WalIterator
	cfCount := [4]int{0,0,0,0}
	for i<count {
		for db.GetLatestSequenceNumber()<=seq {
			fmt.Printf("still at %d\n", db.GetLatestSequenceNumber())
			time.Sleep(time.Millisecond*100)
		}
		if iter==nil {
			iter = db.GetUpdatesSince(seq+1)
			fmt.Printf("reset to %d, status %s\n", seq, iter.Status())
			time.Sleep(time.Millisecond)
		} else {
			iter.Next()
		}
		if !iter.Valid() {
			fmt.Printf("no longer valid: %s\n", iter.Status())
			time.Sleep(time.Millisecond)
			iter.Destroy()
			iter = nil
			continue
		}
		var batch *WriteBatch
		batch, newSeq := iter.Batch()
		if newSeq>seq {
			seq = newSeq
			//fmt.Printf("< %d ", seq)
			for bi := batch.NewIterator(); bi.Next(); {
				rec := bi.Record()
				fmt.Printf("%d '%s' (%d)\n", rec.CF, string(rec.Key), seq)
				i++
				cfCount[rec.CF]++
			}
		} else {
			seq++ // :(
		}
		//fmt.Println()
		batch.Destroy()
	}
	if iter!=nil {
		iter.Destroy()
	}
	fmt.Println("<I THINK IM DONE")
	ensure.DeepEqual(t, i, count)
	ensure.DeepEqual(t, cfCount, [4]int{count>>2,count>>2,count>>2,count>>2,})

	for i:=0; i<len(handles); i++ {
		handles[i].Destroy()
	}
	db.Close()
}

