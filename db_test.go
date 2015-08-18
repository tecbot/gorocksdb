package gorocksdb

import (
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDB(t *testing.T) {
	dbName := os.TempDir() + "/TestDB"

	Convey("Subject: DB", t, func() {
		options := NewDefaultOptions()
		DestroyDb(dbName, options)
		options.SetCreateIfMissing(true)

		db, err := OpenDb(options, dbName)
		So(err, ShouldBeNil)

		Convey("When get bytes, it should return nil or byte slices", func() {
			wo := NewDefaultWriteOptions()
			So(db.Put(wo, []byte("key1"), []byte("value1")), ShouldBeNil)

			ro := NewDefaultReadOptions()
			value, err := db.GetBytes(ro, []byte("key1"))
			So(err, ShouldBeNil)
			So(string(value), ShouldEqual, "value1")

			value, err = db.GetBytes(ro, []byte("key2"))
			So(err, ShouldBeNil)
			So(value, ShouldEqual, nil)
		})
	})
}
