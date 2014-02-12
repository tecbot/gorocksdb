package gorocksdb

import (
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

type testFilterPolicyHandler struct {
	called bool
}

func (self *testFilterPolicyHandler) CreateFilter(keys [][]byte) []byte {
	return make([]byte, len(keys))
}

func (self *testFilterPolicyHandler) KeyMayMatch(key []byte, filter []byte) bool {
	return true
}

func (self *testFilterPolicyHandler) Name() string {
	self.called = true
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
				_, err := OpenDb(options, dbName)
				if err != nil {
					panic(err)
				}
				So(handler.called, ShouldBeTrue)
			})
		})
	})
}
