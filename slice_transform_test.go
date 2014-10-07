package gorocksdb

import (
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

type testSliceTransform struct {
	initiated bool
}

func (self *testSliceTransform) Transform(src []byte) []byte {
	return src[0:3]
}

func (self *testSliceTransform) InDomain(src []byte) bool {
	return len(src) >= 3
}

func (self *testSliceTransform) InRange(src []byte) bool {
	return len(src) == 3
}

func (self *testSliceTransform) Name() string {
	self.initiated = true
	return "gorocksdb.test"
}

func TestCustomSliceTransform(t *testing.T) {
	dbName := os.TempDir() + "/TestNewSliceTransform"

	Convey("Subject: Prefix filtering with custom slice transform", t, func() {
		sliceTransform := &testSliceTransform{}

		options := NewDefaultOptions()
		DestroyDb(dbName, options)

		options.SetPrefixExtractor(sliceTransform)
		options.SetHashSkipListRep(50000, 4, 4)
		options.SetAllowMmapReads(true)
		options.SetAllowMmapWrites(true)
		options.SetPlainTableFactory(4, 10, 0.75, 16)
		options.SetCreateIfMissing(true)

		db, err := OpenDb(options, dbName)
		defer db.Close()

		So(err, ShouldBeNil)

		wo := NewDefaultWriteOptions()
		So(db.Put(wo, []byte("foo1"), []byte("foo")), ShouldBeNil)
		So(db.Put(wo, []byte("foo2"), []byte("foo")), ShouldBeNil)
		So(db.Put(wo, []byte("foo3"), []byte("foo")), ShouldBeNil)
		So(db.Put(wo, []byte("bar1"), []byte("bar")), ShouldBeNil)
		So(db.Put(wo, []byte("bar2"), []byte("bar")), ShouldBeNil)
		So(db.Put(wo, []byte("bar3"), []byte("bar")), ShouldBeNil)

		ro := NewDefaultReadOptions()

		it := db.NewIterator(ro)
		defer it.Close()
		numFound := 0
		for it.Seek([]byte("bar")); it.Valid(); it.Next() {
			numFound++
		}

		So(it.Err(), ShouldBeNil)
		So(numFound, ShouldEqual, 3)
	})
}

func TestFixedPrefixTransform(t *testing.T) {
	dbName := os.TempDir() + "/TestNewFixedPrefixTransform"

	Convey("Subject: Prefix filtering with end condition checking", t, func() {
		options := NewDefaultOptions()
		DestroyDb(dbName, options)

		options.SetHashSkipListRep(50000, 4, 4)
		options.SetAllowMmapReads(true)
		options.SetAllowMmapWrites(true)
		options.SetPlainTableFactory(4, 10, 0.75, 16)
		options.SetCreateIfMissing(true)

		db, err := OpenDb(options, dbName)
		defer db.Close()

		So(err, ShouldBeNil)

		wo := NewDefaultWriteOptions()
		So(db.Put(wo, []byte("foo1"), []byte("foo")), ShouldBeNil)
		So(db.Put(wo, []byte("foo2"), []byte("foo")), ShouldBeNil)
		So(db.Put(wo, []byte("foo3"), []byte("foo")), ShouldBeNil)
		So(db.Put(wo, []byte("bar1"), []byte("bar")), ShouldBeNil)
		So(db.Put(wo, []byte("bar2"), []byte("bar")), ShouldBeNil)
		So(db.Put(wo, []byte("bar3"), []byte("bar")), ShouldBeNil)

		ro := NewDefaultReadOptions()

		it := db.NewIterator(ro)
		defer it.Close()
		numFound := 0
		prefix := []byte("bar")
		// Iterators must now be checked for passing the end condition
		// See https://github.com/facebook/rocksdb/wiki/Prefix-Seek-API-Changes
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			numFound++
		}

		So(it.Err(), ShouldBeNil)
		So(numFound, ShouldEqual, 3)
	})
}
