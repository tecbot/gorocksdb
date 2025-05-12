#!/bin/sh

set -e

if [ ! -f /rocksdb/librocksdb.a ]
then
  rm -rf /rocksdb/*

  git clone git@github.com:GetStream/rocksdb.git /rocksdb && \
    cd /rocksdb && \
    git checkout broadwell && \
    DISABLE_JEMALLOC=1 make static_lib -j5
fi

cd /gorocksdb

exec "$@"