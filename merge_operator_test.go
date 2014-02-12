package gorocksdb

import (
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

type testMergeOperatorHandler struct {
	called bool
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
	self.called = true
	return "gorocksdb.test"
}

func TestNewMergeOperator(t *testing.T) {
	dbName := os.TempDir() + "/TestNewMergeOperator"

	Convey("Subject: Custom merge operator", t, func() {
		Convey("When create a custom merge operator it should not panic", func() {
			handler := &testMergeOperatorHandler{}
			merger := NewMergeOperator(handler)

			Convey("When passed to the db as merge operator it should not panic", func() {
				options := NewDefaultOptions()
				DestroyDb(dbName, options)
				options.SetCreateIfMissing(true)
				options.SetMergeOperator(merger)
				options.SetMaxSuccessiveMerges(5)

				db, err := OpenDb(options, dbName)
				if err != nil {
					panic(err)
				}
				Convey("When merge the value 'foo' with 'bar' then the new value should be 'foobar'", func() {
					wo := NewDefaultWriteOptions()
					err := db.Put(wo, []byte("foo"), []byte("foo"))
					if err != nil {
						panic(err)
					}

					err = db.Merge(wo, []byte("foo"), []byte("bar"))
					if err != nil {
						panic(err)
					}

					value, err := db.Get(NewDefaultReadOptions(), []byte("foo"))
					if err != nil {
						panic(err)
					}

					So(value.Data(), ShouldResemble, []byte("foobar"))
					value.Free()
				})
			})
		})
	})
}
