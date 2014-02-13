package gorocksdb

import (
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

type testMergeOperatorHandler struct {
	initiated bool
}

func (self *testMergeOperatorHandler) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	for _, operand := range operands {
		existingValue = append(existingValue, operand...)
	}

	return existingValue, true
}

func (self *testMergeOperatorHandler) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	return append(leftOperand, rightOperand...), true
}

func (self *testMergeOperatorHandler) Name() string {
	self.initiated = true
	return "gorocksdb.test"
}

func TestNewMergeOperator(t *testing.T) {
	dbName := os.TempDir() + "/TestNewMergeOperator"

	Convey("Subject: Custom merge operator", t, func() {
		Convey("When create a custom merge operator then it should not panic", func() {
			handler := &testMergeOperatorHandler{}
			merger := NewMergeOperator(handler)

			Convey("When passed to the db as merge operator then it should not panic", func() {
				options := NewDefaultOptions()
				DestroyDb(dbName, options)
				options.SetCreateIfMissing(true)
				options.SetMergeOperator(merger)
				options.SetMaxSuccessiveMerges(5)

				db, err := OpenDb(options, dbName)
				So(err, ShouldBeNil)
				So(handler.initiated, ShouldBeTrue)

				Convey("When merge the value 'foo' with 'bar' then the new value should be 'foobar'", func() {
					wo := NewDefaultWriteOptions()
					So(db.Put(wo, []byte("foo"), []byte("foo")), ShouldBeNil)
					So(db.Merge(wo, []byte("foo"), []byte("bar")), ShouldBeNil)

					value, err := db.Get(NewDefaultReadOptions(), []byte("foo"))
					So(err, ShouldBeNil)
					So(value.Data(), ShouldResemble, []byte("foobar"))
					value.Free()
				})
			})
		})
	})
}
