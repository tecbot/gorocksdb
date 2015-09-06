package gorocksdb

import (
	"bytes"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

type testComparator struct {
	numCompared int
	initiated   bool
}

func (self *testComparator) Compare(a, b []byte) int {
	self.numCompared++

	return bytes.Compare(a, b)
}

func (self *testComparator) Name() string {
	self.initiated = true
	return "gorocksdb.test"
}

func TestNewComparator(t *testing.T) {
	dbName := os.TempDir() + "/TestNewComparator"

	Convey("Subject: Custom comparator", t, func() {
		Convey("When passed to the db as comperator then it should not panic", func() {
			cmp := &testComparator{}
			options := NewDefaultOptions()
			DestroyDb(dbName, options)
			options.SetCreateIfMissing(true)
			options.SetComparator(cmp)

			db, err := OpenDb(options, dbName)
			So(err, ShouldBeNil)
			So(cmp.initiated, ShouldBeTrue)

			Convey("When put 3 values into the db then the comperator should be called two times", func() {
				wo := NewDefaultWriteOptions()
				So(db.Put(wo, []byte("key1"), []byte("value1")), ShouldBeNil)
				So(db.Put(wo, []byte("key2"), []byte("value2")), ShouldBeNil)
				So(db.Put(wo, []byte("key3"), []byte("value3")), ShouldBeNil)
				So(cmp.numCompared, ShouldEqual, 2)
			})
		})
	})
}
