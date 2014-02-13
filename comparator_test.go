package gorocksdb

import (
	"bytes"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

type testComparatorHandler struct {
	numCompared int
	initiated   bool
}

func (self *testComparatorHandler) Compare(a, b []byte) int {
	self.numCompared++

	return bytes.Compare(a, b)
}

func (self *testComparatorHandler) Name() string {
	self.initiated = true
	return "gorocksdb.test"
}

func TestNewComparator(t *testing.T) {
	dbName := os.TempDir() + "/TestNewComparator"

	Convey("Subject: Custom comparator", t, func() {
		Convey("When create a custom comparator then it should not panic", func() {
			handler := &testComparatorHandler{}
			cmp := NewComparator(handler)

			Convey("When passed to the db as comperator then it should not panic", func() {
				options := NewDefaultOptions()
				DestroyDb(dbName, options)
				options.SetCreateIfMissing(true)
				options.SetComparator(cmp)

				db, err := OpenDb(options, dbName)
				So(err, ShouldBeNil)
				So(handler.initiated, ShouldBeTrue)

				Convey("When put 3 values into the db then the comperator should be called two times", func() {
					wo := NewDefaultWriteOptions()
					So(db.Put(wo, []byte("key1"), []byte("value1")), ShouldBeNil)
					So(db.Put(wo, []byte("key2"), []byte("value2")), ShouldBeNil)
					So(db.Put(wo, []byte("key3"), []byte("value3")), ShouldBeNil)
					So(handler.numCompared, ShouldEqual, 2)
				})
			})
		})
	})
}
