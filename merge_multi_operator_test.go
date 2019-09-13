package gorocksdb

import (
	"testing"

	"github.com/facebookgo/ensure"
)

func TestMergeMultiOperator(t *testing.T) {
	var (
		givenKey     = []byte("hello")
		startingVal  = []byte("foo")
		mergeVal1    = []byte("bar")
		mergeVal2    = []byte("baz")
		fMergeResult = []byte("foobarbaz")
		pMergeResult = []byte("barbaz")
	)

	merger := &mockMergeMultiOperator{
		fullMerge: func(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
			ensure.DeepEqual(&fatalAsError{t}, key, givenKey)
			ensure.DeepEqual(&fatalAsError{t}, existingValue, startingVal)
			ensure.DeepEqual(&fatalAsError{t}, operands[0], pMergeResult)
			return fMergeResult, true
		},
		partialMerge: func(key, leftOperand, rightOperand []byte) ([]byte, bool) {
			t.FailNow() // this should never be called
			return nil, false
		},
		partialMergeMulti: func(key []byte, operands [][]byte) ([]byte, bool) {
			ensure.DeepEqual(&fatalAsError{t}, key, givenKey)
			ensure.DeepEqual(&fatalAsError{t}, operands[0], mergeVal1)
			ensure.DeepEqual(&fatalAsError{t}, operands[1], mergeVal2)
			return pMergeResult, true
		},
	}
	db := newTestDB(t, "TestMergeOperator", func(opts *Options) {
		opts.SetMergeOperator(merger)
	})
	defer db.Close()

	wo := NewDefaultWriteOptions()
	defer wo.Destroy()

	// insert a starting value and compact to trigger merges
	ensure.Nil(t, db.Put(wo, givenKey, startingVal))

	// trigger a compaction to ensure that a merge is performed
	db.CompactRange(Range{nil, nil})

	// we expect these two operands to be passed to merge multi
	ensure.Nil(t, db.Merge(wo, givenKey, mergeVal1))
	ensure.Nil(t, db.Merge(wo, givenKey, mergeVal2))

	// trigger a compaction to ensure that a
	// partial and full merge are performed
	db.CompactRange(Range{nil, nil})

	ro := NewDefaultReadOptions()
	v1, err := db.Get(ro, givenKey)
	defer v1.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v1.Data(), fMergeResult)

}

type mockMergeMultiOperator struct {
	fullMerge         func(key, existingValue []byte, operands [][]byte) ([]byte, bool)
	partialMerge      func(key, leftOperand, rightOperand []byte) ([]byte, bool)
	partialMergeMulti func(key []byte, operands [][]byte) ([]byte, bool)
}

func (m *mockMergeMultiOperator) Name() string { return "gorocksdb.test" }
func (m *mockMergeMultiOperator) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	return m.fullMerge(key, existingValue, operands)
}
func (m *mockMergeMultiOperator) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	return m.partialMerge(key, leftOperand, rightOperand)
}
func (m *mockMergeMultiOperator) PartialMergeMulti(key []byte, operands [][]byte) ([]byte, bool) {
	return m.partialMergeMulti(key, operands)
}
