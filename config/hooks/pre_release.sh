#!/bin/sh

## Copy leveldb and rocksdb libraries
SLIB=config/slib/
mkdir -p $SLIB
cp $REBAR_DEPS_DIR/erl_rocksdb/c_src/rocksdb/librocksdb.* $SLIB
cp $REBAR_DEPS_DIR/erl_leveldb/c_src/leveldb/libleveldb.* $SLIB
# End
