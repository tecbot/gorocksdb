package gorocksdb

import (
	"testing"

	"github.com/facebookgo/ensure"
)

func TestMergeOperator(t *testing.T) {
	var (
		givenKey    = []byte("hello")
		givenVal1   = []byte("foo")
		givenVal2   = []byte("bar")
		givenMerged = []byte("foobar")
	)
	merger := NewMockMergeOperator(func(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
		ensure.DeepEqual(&fatalAsError{t}, key, givenKey)
		ensure.DeepEqual(&fatalAsError{t}, existingValue, givenVal1)
		ensure.DeepEqual(&fatalAsError{t}, operands, [][]byte{givenVal2})
		return givenMerged, true
	}, nil)

	db := newTestDB(t, "TestMergeOperator", func(opts *Options) {
		opts.SetMergeOperator(merger)
	})
	defer db.Close()

	wo := NewDefaultWriteOptions()
	ensure.Nil(t, db.Put(wo, givenKey, givenVal1))
	ensure.Nil(t, db.Merge(wo, givenKey, givenVal2))

	// trigger a compaction to ensure that a merge is performed
	db.CompactRange(Range{nil, nil})

	ro := NewDefaultReadOptions()
	v1, err := db.Get(ro, givenKey)
	defer v1.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v1.Data(), givenMerged)
}
