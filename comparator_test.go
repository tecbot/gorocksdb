package gorocksdb

import (
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

type testComparatorHandler struct {
	called bool
}

func (self *testComparatorHandler) Compare(a []byte, b []byte) int {
	return 0
}

func (self *testComparatorHandler) Name() string {
	self.called = true
	return "gorocksdb.test"
}

func TestNewComparator(t *testing.T) {
	dbName := os.TempDir() + "/TestNewComparator"

	Convey("Subject: Custom comparator", t, func() {
		Convey("When create a custom comparator it should not panic", func() {
			handler := &testComparatorHandler{}
			cmp := NewComparator(handler)

			Convey("When passed to the db as comperator it should not panic", func() {
				options := NewDefaultOptions()
				DestroyDb(dbName, options)
				options.SetCreateIfMissing(true)
				options.SetComparator(cmp)
				_, err := OpenDb(options, dbName)
				if err != nil {
					panic(err)
				}
				So(handler.called, ShouldBeTrue)
			})
		})
	})
}
