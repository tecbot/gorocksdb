#!/bin/sh
# This script will attempt to migrate all files from rocksdb as can be
# downloaded from https://github.com/facebook/rocksdb/releases

# This is not gauranteed to be perfect but it is definitely a head
# start. The error messages (if there are any) are mostly workable and I'll attempt
# to use this script to migrate the next release and improve upon it.
# Since I've made the base though this should work pretty well.

# note: sudo apt-get install silversearcher-ag (also in brew, diff name I believe)
# feel free to rewrite for git grep, normal grep is slow :/

# PRECONDITION: $PWD=/path/to/gorocksdb

if [ "$1" == "" ]; then
  echo "give directory for rocksdb source as arg"
  exit 2
fi

rocksdir=$1 # root of rocksdb, relative or abs

# get all newer .cc files we need from lib
for f in *.cc; do newer=$(ag -g "$f" $rocksdir) && 
  if [[ -n $newer ]]; 
    then mv "$newer $f";
  fi;
done

# see rocksdb_env.cc
mv $rocksdir/util/env_posix.cc ./port
# see rocksdb_port.cc
mv $rocksdir/port/port_posix.cc ./port

# put headers in appropriate places
for f in {db,port,rocksdb,hdfs,table,util,utilities}/*.h; 
  do newer=$(ag -g "$f" $rocksdir) && 
  if [[ -n $newer ]]; 
    then mv "$newer $f";
  fi;
done

# TODO anything I missed...
