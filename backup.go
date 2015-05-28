package gorocksdb

// #cgo LDFLAGS: -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy
// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"

import (
	"errors"
	"unsafe"
)

type BackupEngineInfo struct {
    c *C.rocksdb_backup_engine_info_t
}

func (self *BackupEngineInfo) GetCount() int {
    return int(C.rocksdb_backup_engine_info_count(self.c))
}

// BackupEngine is a reusable handle to a RocksDB Backup, created by
// OpenBackupEngine
type BackupEngine struct {
    c *C.rocksdb_backup_engine_t
    path string
    opts *Options
}


// OpenBackupEngine opens a backup engine with specified options
func OpenBackupEngine(opts *Options, path string) (*BackupEngine, error) {
	var cErr *C.char
    cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

    be := C.rocksdb_backup_engine_open(opts.c, cpath, &cErr)
    if cErr != nil {
        defer C.free(unsafe.Pointer(cErr))
        return nil, errors.New(C.GoString(cErr))
    }

    return &BackupEngine{
        c: be,
        path: path,
        opts: opts,
    }, nil
}

func (self *BackupEngine) CreateNewBackup(db *DB) error {
	var cErr *C.char

    C.rocksdb_backup_engine_create_new_backup(self.c, db.c, &cErr)
    if cErr != nil {
        defer C.free(unsafe.Pointer(cErr))
        return errors.New(C.GoString(cErr))
    }

    return nil
}

func (self *BackupEngine) GetInfo() *BackupEngineInfo {
    return &BackupEngineInfo{
        c: C.rocksdb_backup_engine_get_backup_info(self.c),
    }
}
