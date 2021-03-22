// +build !rocksdb_6_16

package gorocksdb

import (
	"testing"

	"github.com/facebookgo/ensure"
)

func TestDBGetApproximateSizes(t *testing.T) {
	db := newTestDB(t, "TestDBGetApproximateSizes", nil)
	defer db.Close()

	// no ranges
	sizes := db.GetApproximateSizes(nil)
	ensure.DeepEqual(t, len(sizes), 0)

	// range will nil start and limit
	sizes = db.GetApproximateSizes([]Range{{Start: nil, Limit: nil}})
	ensure.DeepEqual(t, sizes, []uint64{0})

	// valid range
	sizes = db.GetApproximateSizes([]Range{{Start: []byte{0x00}, Limit: []byte{0xFF}}})
	ensure.DeepEqual(t, sizes, []uint64{0})
}

func TestDBGetApproximateSizesCF(t *testing.T) {
	db := newTestDB(t, "TestDBGetApproximateSizesCF", nil)
	defer db.Close()

	o := NewDefaultOptions()

	cf, err := db.CreateColumnFamily(o, "other")
	ensure.Nil(t, err)

	// no ranges
	sizes := db.GetApproximateSizesCF(cf, nil)
	ensure.DeepEqual(t, len(sizes), 0)

	// range will nil start and limit
	sizes = db.GetApproximateSizesCF(cf, []Range{{Start: nil, Limit: nil}})
	ensure.DeepEqual(t, sizes, []uint64{0})

	// valid range
	sizes = db.GetApproximateSizesCF(cf, []Range{{Start: []byte{0x00}, Limit: []byte{0xFF}}})
	ensure.DeepEqual(t, sizes, []uint64{0})
}
