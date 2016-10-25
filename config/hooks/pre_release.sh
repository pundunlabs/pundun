#!/bin/sh

## Copy leveldb libraries
SLIB=config/slib/
mkdir -p $SLIB
cp $REBAR_DEPS_DIR/erl_leveldb/c_src/leveldb/libleveldb.* $SLIB
# End
