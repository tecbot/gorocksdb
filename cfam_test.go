package gorocksdb

import (
	"os"
	"testing"
	. "github.com/smartystreets/goconvey/convey"
)

func TestColumnFamilies(t *testing.T) {
	dbName := os.TempDir() + "/TestCF"

	Convey("Subject: Column Fams", t, func() {

		dbopts := NewDefaultOptions()
		DestroyDb(dbName, dbopts)

		dbopts.SetCreateIfMissing(true)
		cfopts := NewDefaultOptions()
		cfd := ColumnFamilyDescriptor{"cf1", cfopts}

		db, err := OpenDb(dbopts, dbName)
		cf1, err := db.CreateColumnFamily(cfd)
		So(err, ShouldBeNil)
		cf1.Destroy()

		db.Close()

		names, err := ListColumnFamilies(dbopts, dbName)
		So(names[0], ShouldEqual, "default")
		So(names[1], ShouldEqual, "cf1")

		defcf := ColumnFamilyDescriptor{"default", cfopts}
		cfds := []ColumnFamilyDescriptor{defcf, cfd}
		db, cfs, err := OpenDbWithColumnFamilies(dbopts, dbName, cfds)
		So(err, ShouldBeNil)
		So(len(cfs), ShouldEqual, 2)

		missingdb := os.TempDir() + "/TestMissingCF"
		opts := NewDefaultOptions()
		opts.SetCreateIfMissing(true)
		opts.SetCreateMissingColumnFamilies(true)
		DestroyDb(missingdb, opts)
		testdb, cfmissing, err := OpenDbWithColumnFamilies(opts, missingdb, cfds)
		So(err, ShouldBeNil)
		So(len(cfmissing), ShouldEqual, 2)
		testdb.Close()

		wo := NewDefaultWriteOptions()
		err = db.PutCF(wo, cfs[1], []byte("hi"), []byte("world"))
		So(err, ShouldBeNil)

		ro := NewDefaultReadOptions()
		d, err := db.GetCF(ro, cfs[1], []byte("hi"))
		So(err, ShouldBeNil)
		So(d.Data(), ShouldResemble, []byte("world"))
		d.Free()

		err = db.DeleteCF(wo, cfs[1], []byte("hi"))
		So(err, ShouldBeNil)

		d, err = db.GetCF(ro, cfs[1], []byte("hi"))
		So(err, ShouldBeNil)
		So(d.Data(), ShouldBeNil)
		d.Free()

		wb := NewWriteBatch()
		defer wb.Destroy()

		wb.PutCF(cfs[1], []byte("key1"), []byte("value1"))
		wb.PutCF(cfs[1], []byte("key2"), []byte("value2"))
		wb.DeleteCF(cfs[1], []byte("key3"))
		So(wb.Count(), ShouldEqual, 3)

		err = db.Write(wo, wb)
		So(err, ShouldBeNil)

		value, err := db.GetCF(ro, cfs[1], []byte("key1"))
		So(err, ShouldBeNil)
		So(value.Data(), ShouldResemble, []byte("value1"))
		value.Free()

		value, err = db.GetCF(ro, cfs[1], []byte("key2"))
		So(err, ShouldBeNil)
		So(value.Data(), ShouldResemble, []byte("value2"))
		value.Free()

		value, err = db.GetCF(ro, cfs[1], []byte("key3"))
		So(err, ShouldBeNil)
		So(value.Size(), ShouldEqual, 0)
		value.Free()

		for _, cf := range cfs {
			cf.Destroy()
		}
		db.Close()
	})
}
