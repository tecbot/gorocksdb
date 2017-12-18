package gorocksdb

import (
	"testing"

	"github.com/facebookgo/ensure"
)

func TestSliceTransform(t *testing.T) {
	db := newTestDB(t, "TestSliceTransform", func(opts *Options) {
		opts.SetPrefixExtractor(NewMockSliceTransform())
	})
	defer db.Close()

	wo := NewDefaultWriteOptions()
	ensure.Nil(t, db.Put(wo, []byte("foo1"), []byte("foo")))
	ensure.Nil(t, db.Put(wo, []byte("foo2"), []byte("foo")))
	ensure.Nil(t, db.Put(wo, []byte("bar1"), []byte("bar")))

	iter := db.NewIterator(NewDefaultReadOptions())
	defer iter.Close()
	prefix := []byte("foo")
	numFound := 0
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		numFound++
	}
	ensure.Nil(t, iter.Err())
	ensure.DeepEqual(t, numFound, 2)
}

func TestFixedPrefixTransformOpen(t *testing.T) {
	db := newTestDB(t, "TestFixedPrefixTransformOpen", func(opts *Options) {
		opts.SetPrefixExtractor(NewFixedPrefixTransform(3))
	})
	defer db.Close()
}
