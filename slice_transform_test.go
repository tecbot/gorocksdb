package gorocksdb

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

type testSliceTransformHandler struct {
	initiated bool
}

func (self *testSliceTransformHandler) Transform(src []byte) []byte {
	fmt.Println("Transform", string(src))
	return src
}

func (self *testSliceTransformHandler) InDomain(src []byte) bool {
	fmt.Println("InDomain", string(src))
	return true
}

func (self *testSliceTransformHandler) InRange(src []byte) bool {
	fmt.Println(src)
	return true
}

func (self *testSliceTransformHandler) Name() string {
	self.initiated = true
	return "gorocksdb.test"
}

func TestNewSliceTransform(t *testing.T) {
	dbName := os.TempDir() + "/TestNewSliceTransform"

	Convey("Subject: Custom slice transform", t, func() {
		Convey("When create a custom slice transform it should not panic", func() {
			handler := &testSliceTransformHandler{}
			sliceTransform := NewSliceTransform(handler)

			Convey("When passed to the db as prefix extractor it should not panic", func() {
				options := NewDefaultOptions()
				DestroyDb(dbName, options)
				options.SetCreateIfMissing(true)
				options.SetPrefixExtractor(sliceTransform)
				options.SetWholeKeyFiltering(true)

				_, err := OpenDb(options, dbName)
				So(err, ShouldBeNil)
				So(handler.initiated, ShouldBeTrue)
			})
		})
	})
}

func TestNewFixedPrefixTransform(t *testing.T) {
	dbName := os.TempDir() + "/TestNewFixedPrefixTransform"

	Convey("Subject: Create fixed prefix transform", t, func() {
		Convey("When create a fixed prefix transform then it should not panic", func() {
			sliceTransform := NewFixedPrefixTransform(3)

			Convey("When passed to the db as prefix extractor then it should not panic", func() {
				options := NewDefaultOptions()
				DestroyDb(dbName, options)
				options.SetCreateIfMissing(true)
				options.SetPrefixExtractor(sliceTransform)
				options.SetWholeKeyFiltering(true)
				options.SetFilterPolicy(NewBloomFilter(8))

				db, err := OpenDb(options, dbName)
				So(err, ShouldBeNil)

				Convey("When add 3 values with key prefix 'foo' and 3 values with key prefix 'bar' then it should not panic", func() {
					wo := NewDefaultWriteOptions()
					So(db.Put(wo, []byte("foo1"), []byte("foo")), ShouldBeNil)
					So(db.Put(wo, []byte("foo2"), []byte("foo")), ShouldBeNil)
					So(db.Put(wo, []byte("foo3"), []byte("foo")), ShouldBeNil)
					So(db.Put(wo, []byte("bar1"), []byte("bar")), ShouldBeNil)
					So(db.Put(wo, []byte("bar2"), []byte("bar")), ShouldBeNil)
					So(db.Put(wo, []byte("bar3"), []byte("bar")), ShouldBeNil)

					Convey("When create an interator and seek to prefix 'bar' then it should return only 3 keys", func() {
						ro := NewDefaultReadOptions()
						ro.SetPrefixSeek(true)
						ro.SetPrefix([]byte("bar"))

						it := db.NewIterator(ro)
						it.Seek([]byte("bar"))
						numFound := 0
						for ; it.Valid(); it.Next() {
							numFound++

						}
						So(it.Err(), ShouldBeNil)
						So(numFound, ShouldEqual, 3)
						it.Close()
					})
				})
			})
		})
	})
}
