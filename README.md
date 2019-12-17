# gorocksdb, a Go wrapper for RocksDB

[![Build Status](https://travis-ci.org/tecbot/gorocksdb.svg)](https://travis-ci.org/tecbot/gorocksdb) [![GoDoc](https://godoc.org/github.com/tecbot/gorocksdb?status.svg)](http://godoc.org/github.com/tecbot/gorocksdb)

## Install

You'll need to build [RocksDB](https://github.com/facebook/rocksdb) v6.0+  on your machine.

After that, you can install gorocksdb using the following command:

    CGO_CFLAGS="-I/path/to/rocksdb/include" \
    CGO_LDFLAGS="-L/path/to/rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd -ldl" \
      go get github.com/tecbot/gorocksdb

Please note that this package might upgrade the required RocksDB version at any moment.
Vendoring is thus highly recommended if you require high stability.

*The [embedded CockroachDB RocksDB](https://github.com/cockroachdb/c-rocksdb) is no longer supported in gorocksdb.*


## Previous Versions

The master branch may not be version stable.  If you are looking for a release that is stable for a particular version of rocksdb, use the following compatibility chart:

| gorocksdb version | rocksdb major release compatibility | rocksdb minimum version |
|:---:|:---:|:---:|
| v1.0 | 5.X | 5.16 |
| v2.0 | 6.X | 6.0 |