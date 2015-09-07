package gorocksdb

import (
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestIterator(t *testing.T) {
	dbName := os.TempDir() + "/TestIterator"

	Convey("Subject: Iterator", t, func() {
		options := NewDefaultOptions()
		DestroyDb(dbName, options)
		options.SetCreateIfMissing(true)

		db, err := OpenDb(options, dbName)
		So(err, ShouldBeNil)

		Convey("When freeing iterator data, it should not panic", func() {
			wo := NewDefaultWriteOptions()
			So(db.Put(wo, []byte("key1"), []byte("value1")), ShouldBeNil)
			So(db.Put(wo, []byte("key2"), []byte("value2")), ShouldBeNil)

			ro := NewDefaultReadOptions()
			iter := db.NewIterator(ro)
			iter.Seek(nil)
			So(iter.Valid(), ShouldBeTrue)
			key := iter.Key()
			So(string(key.Data()), ShouldEqual, "key1")
			key.Free()
			val := iter.Value()
			So(string(val.Data()), ShouldEqual, "value1")
			val.Free()

			iter.Next()
			So(iter.Valid(), ShouldBeTrue)
			key = iter.Key()
			So(string(key.Data()), ShouldEqual, "key2")
			key.Free()
			val = iter.Value()
			So(string(val.Data()), ShouldEqual, "value2")
			val.Free()

			iter.Next()
			So(iter.Valid(), ShouldBeFalse)
		})
	})
}
