package gorocksdb

import (
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestWriteBatch(t *testing.T) {
	dbName := os.TempDir() + "/TestNewWriteBatch"

	Convey("Subject: Batching of db operations using a write batch", t, func() {
		options, ro, wo := NewDefaultOptions(), NewDefaultReadOptions(), NewDefaultWriteOptions()
		DestroyDb(dbName, options)
		options.SetCreateIfMissing(true)
		db, err := OpenDb(options, dbName)
		So(err, ShouldBeNil)
		defer db.Close()

		err = db.Put(wo, []byte("key3"), []byte("value3"))
		So(err, ShouldBeNil)

		wb := NewWriteBatch()
		defer wb.Destroy()

		wb.Put([]byte("key1"), []byte("value1"))
		wb.Put([]byte("key2"), []byte("value2"))
		wb.Delete([]byte("key3"))
		So(wb.Count(), ShouldEqual, 3)

		err = db.Write(wo, wb)
		So(err, ShouldBeNil)

		value, err := db.Get(ro, []byte("key1"))
		So(err, ShouldBeNil)
		So(value.Data(), ShouldResemble, []byte("value1"))
		value.Free()

		value, err = db.Get(ro, []byte("key2"))
		So(err, ShouldBeNil)
		So(value.Data(), ShouldResemble, []byte("value2"))
		value.Free()

		value, err = db.Get(ro, []byte("key3"))
		So(err, ShouldBeNil)
		So(value.Size(), ShouldEqual, 0)
		value.Free()
	})
}

func TestWriteBatchIterator(t *testing.T) {
	dbName := os.TempDir() + "/TestWriteBatchIterator"

	Convey("Subject: Iterate over a write batch", t, func() {
		options := NewDefaultOptions()
		DestroyDb(dbName, options)
		options.SetCreateIfMissing(true)
		db, err := OpenDb(options, dbName)
		So(err, ShouldBeNil)
		defer db.Close()

		wb := NewWriteBatch()
		defer wb.Destroy()

		wb.Put([]byte("key1"), []byte("value1"))
		wb.Put([]byte("key2"), []byte("value2"))
		wb.Delete([]byte("key3"))
		So(wb.Count(), ShouldEqual, 3)

		iter := wb.NewIterator()
		So(iter.Next(), ShouldBeTrue)

		record := iter.Record()
		So(record, ShouldNotBeNil)
		So(record.Key, ShouldResemble, []byte("key1"))
		So(record.Value, ShouldResemble, []byte("value1"))
		So(record.Type, ShouldEqual, WriteBatchRecordTypeValue)

		So(iter.Next(), ShouldBeTrue)
		record = iter.Record()
		So(record, ShouldNotBeNil)
		So(record.Key, ShouldResemble, []byte("key2"))
		So(record.Value, ShouldResemble, []byte("value2"))
		So(record.Type, ShouldEqual, WriteBatchRecordTypeValue)

		So(iter.Next(), ShouldBeTrue)
		record = iter.Record()
		So(record, ShouldNotBeNil)
		So(record.Key, ShouldResemble, []byte("key3"))
		So(record.Type, ShouldEqual, WriteBatchRecordTypeDeletion)

		So(iter.Next(), ShouldBeFalse)
	})
}
