package gorocksdb

import (
	"bytes"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

type testFilterPolicyHandler struct {
	numKeys   int
	initiated bool
}

func (self *testFilterPolicyHandler) CreateFilter(keys [][]byte) []byte {
	filter := []byte{}
	for _, key := range keys {
		filter = append(filter, key...)
		self.numKeys++
	}

	return filter
}

func (self *testFilterPolicyHandler) KeyMayMatch(key []byte, filter []byte) bool {
	return bytes.Contains(filter, key)
}

func (self *testFilterPolicyHandler) Name() string {
	self.initiated = true
	return "gorocksdb.test"
}

func TestNewFilterPolicy(t *testing.T) {
	dbName := os.TempDir() + "/TestNewFilterPolicy"

	Convey("Subject: Custom filter policy", t, func() {
		Convey("When create a custom filter policy it should not panic", func() {
			handler := &testFilterPolicyHandler{}
			policy := NewFilterPolicy(handler)

			Convey("When passed to the db as filter policy it should not panic", func() {
				options := NewDefaultOptions()
				DestroyDb(dbName, options)
				options.SetCreateIfMissing(true)
				options.SetFilterPolicy(policy)

				db, err := OpenDb(options, dbName)
				So(err, ShouldBeNil)
				So(handler.initiated, ShouldBeTrue)

				Convey("When put 3 key to the db then the filter should receive 3 keys after a flush", func() {
					wo := NewDefaultWriteOptions()
					So(db.Put(wo, []byte("key1"), []byte("value1")), ShouldBeNil)
					So(db.Put(wo, []byte("key2"), []byte("value2")), ShouldBeNil)
					So(db.Put(wo, []byte("key3"), []byte("value3")), ShouldBeNil)
					So(db.Flush(NewDefaultFlushOptions()), ShouldBeNil)
					So(handler.numKeys, ShouldEqual, 3)
				})
			})
		})
	})
}
