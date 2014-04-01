package gorocksdb

import (
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

type testSliceTransformHandler struct {
	initiated bool
}

func (self *testSliceTransformHandler) Transform(src []byte) []byte {
	return src[0:3]
}

func (self *testSliceTransformHandler) InDomain(src []byte) bool {
	return len(src) >= 3
}

func (self *testSliceTransformHandler) InRange(src []byte) bool {
	return len(src) == 3
}

func (self *testSliceTransformHandler) Name() string {
	self.initiated = true
	return "gorocksdb.test"
}

func TestCustomSliceTransform(t *testing.T) {
	dbName := os.TempDir() + "/TestNewSliceTransform"

	Convey("Subject: Prefix filtering with custom slice transform", t, func() {
		handler := &testSliceTransformHandler{}
		sliceTransform := NewSliceTransform(handler)

		options := NewDefaultOptions()
		DestroyDb(dbName, options)

		options.SetFilterPolicy(NewBloomFilter(10))
		options.SetPrefixExtractor(sliceTransform)
		options.SetHashSkipListRep(50000, 4, 4)
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
		ro.SetPrefixSeek(true)

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

	Convey("Subject: Prefix filtering with native fixed prefix transform", t, func() {
		options := NewDefaultOptions()
		DestroyDb(dbName, options)

		options.SetFilterPolicy(NewBloomFilter(10))
		options.SetPrefixExtractor(NewFixedPrefixTransform(3))
		options.SetHashSkipListRep(50000, 4, 4)
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
		ro.SetPrefixSeek(true)

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
