// +build rocksdb_6_16

package gorocksdb

import (
	"testing"

	"github.com/facebookgo/ensure"
)

func TestDBGetApproximateSizes(t *testing.T) {
	db := newTestDB(t, "TestDBGetApproximateSizes", nil)
	defer db.Close()

	// no ranges
	sizes, err := db.GetApproximateSizes(nil)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(sizes), 0)

	// range will nil start and limit
	sizes, err = db.GetApproximateSizes([]Range{{Start: nil, Limit: nil}})
	ensure.Nil(t, err)
	ensure.DeepEqual(t, sizes, []uint64{0})

	// valid range
	sizes, err = db.GetApproximateSizes([]Range{{Start: []byte{0x00}, Limit: []byte{0xFF}}})
	ensure.Nil(t, err)
	ensure.DeepEqual(t, sizes, []uint64{0})
}

func TestDBGetApproximateSizesCF(t *testing.T) {
	db := newTestDB(t, "TestDBGetApproximateSizesCF", nil)
	defer db.Close()

	o := NewDefaultOptions()

	cf, err := db.CreateColumnFamily(o, "other")
	ensure.Nil(t, err)

	// no ranges
	sizes, err := db.GetApproximateSizesCF(cf, nil)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(sizes), 0)

	// range will nil start and limit
	sizes, err = db.GetApproximateSizesCF(cf, []Range{{Start: nil, Limit: nil}})
	ensure.Nil(t, err)
	ensure.DeepEqual(t, sizes, []uint64{0})

	// valid range
	sizes, err = db.GetApproximateSizesCF(cf, []Range{{Start: []byte{0x00}, Limit: []byte{0xFF}}})
	ensure.Nil(t, err)
	ensure.DeepEqual(t, sizes, []uint64{0})
}
