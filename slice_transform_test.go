package gorocksdb

import (
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

type testSliceTransformHandler struct {
	called bool
}

func (self *testSliceTransformHandler) Transform(src []byte) []byte {
	return src
}

func (self *testSliceTransformHandler) InDomain(src []byte) bool {
	return true
}

func (self *testSliceTransformHandler) InRange(src []byte) bool {
	return true
}

func (self *testSliceTransformHandler) Name() string {
	self.called = true
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
				_, err := OpenDb(options, dbName)
				if err != nil {
					panic(err)
				}

				So(handler.called, ShouldBeTrue)
			})
		})
	})
}

func TestNewFixedPrefixTransform(t *testing.T) {
	dbName := os.TempDir() + "/TestNewFixedPrefixTransform"

	Convey("Subject: Create fixed prefix transform", t, func() {
		Convey("When create a fixed prefix transform it should not panic", func() {
			sliceTransform := NewFixedPrefixTransform(8)

			Convey("When passed to the db as prefix extractor it should not panic", func() {
				options := NewDefaultOptions()
				DestroyDb(dbName, options)
				options.SetCreateIfMissing(true)
				options.SetPrefixExtractor(sliceTransform)
				_, err := OpenDb(options, dbName)
				if err != nil {
					panic(err)
				}
				So(err, ShouldBeNil)
			})
		})
	})
}
