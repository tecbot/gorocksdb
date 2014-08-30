package gorocksdb

import (
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

type testCompactionFilter struct{}

func (self *testCompactionFilter) Name() string { return "gorocksdb.test" }
func (self *testCompactionFilter) Filter(_ int, key, _ []byte) (bool, []byte) {
	switch string(key) {
	case "key2":
		return true, nil
	case "key3":
		return false, []byte("new-value")
	}
	return false, nil
}

func TestNewCompactionFilter(t *testing.T) {
	dbName := os.TempDir() + "/TestNewCompactionFilter"

	Convey("Subject: Custom compaction filter", t, func() {
		Convey("When passed to the db then it should not panic", func() {
			filter := &testCompactionFilter{}
			options := NewDefaultOptions()
			DestroyDb(dbName, options)
			options.SetCreateIfMissing(true)
			options.SetCompactionFilter(filter)

			db, err := OpenDb(options, dbName)
			So(err, ShouldBeNil)

			Convey("When compaction is running, should filter/update values", func() {
				wo := NewDefaultWriteOptions()
				So(db.Put(wo, []byte("key1"), []byte("value1")), ShouldBeNil)
				So(db.Put(wo, []byte("key2"), []byte("value2")), ShouldBeNil)
				So(db.Put(wo, []byte("key3"), []byte("value3")), ShouldBeNil)
				db.CompactRange(Range{nil, nil})

				ro := NewDefaultReadOptions()
				s1, err := db.Get(ro, []byte("key1"))
				So(err, ShouldBeNil)
				So(string(s1.Data()), ShouldEqual, "value1")

				s2, err := db.Get(ro, []byte("key2"))
				So(err, ShouldBeNil)
				So(string(s2.Data()), ShouldEqual, "")

				s3, err := db.Get(ro, []byte("key3"))
				So(err, ShouldBeNil)
				So(string(s3.Data()), ShouldEqual, "new-value")
			})
		})
	})
}
