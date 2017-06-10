package gorocksdb

import (
	"github.com/facebookgo/ensure"
	"os"
	"testing"
)

func TestCheckpoint(t *testing.T) {
	dir := "gorocksdbcheckpoint"
	defer os.RemoveAll(dir)
	db := newTestDB(t, "TestCheckpoint", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val")))
	}

	var err error
	var checkpoint *Checkpoint

	checkpoint, err = db.NewCheckpoint()
	defer checkpoint.Close()
	ensure.NotNil(t, checkpoint)
	ensure.Nil(t, err)

	err = checkpoint.CreateCheckpoint(dir, 0)
	ensure.Nil(t, err)
}
