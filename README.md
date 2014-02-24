# gorocksdb

gorocksdb is a Go wrapper for RocksDB.

The API has been godoc'ed and [is available on the
web](http://godoc.org/github.com/tecbot/gorocksdb).

## Building

You'll need the shared library build of
[RocksDB](https://github.com/facebook/rocksdb) installed on your machine, simply run:

    make shared_lib

Now, if you build RocksDB you can install gorocksdb:

    CGO_CFLAGS="-I/path/to/rocksdb/include" CGO_LDFLAGS="-L/path/to/rocksdb" go get github.com/tecbot/gorocksdb