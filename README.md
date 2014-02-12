# gorocksdb

gorocksdb is a Go wrapper for RocksDB.

The API has been godoc'ed and [is available on the
web](http://godoc.org/github.com/tecbot/gorocksdb).


## Building

**Currently the lib is only compatible with the following rocksdb repository** https://github.com/tecbot/rocksdb

    CGO_CFLAGS="-I/path/to/rocksdb/include" CGO_LDFLAGS="-L/path/to/rocksdb" go get github.com/tecbot/gorocksdb