#!/bin/sh

OS_NAME=$(uname -s)

if [ $OS_NAME = "Darwin" ] ; then
    alias sed="sed -i '' "
else
    alias sed='sed -i '
fi

## Use sed to inject customized code in start scripts.
START_SCRIPTS=`find $REBAR_BUILD_DIR/rel -type f -name pundun`
for s in $START_SCRIPTS; do

## SET LD_LIBRARY_PATH
    sed '/export LD_LIBRARY_PATH/i \
LD_DIRS=`find $RELEASE_ROOT_DIR/slib -type d` \
LIBRARY_PATHS=\
for i in $LD_DIRS; do\
    LIBRARY_PATHS="$i:$LIBRARY_PATHS"\
done\
' $s
    sed 's/$LD_LIBRARY_PATH*/$LIBRARY_PATHS:$LD_LIBRARY_PATH/' $s
    sed '/export LD_LIBRARY_PATH/a \
export DYLD_LIBRARY_PATH=$LD_LIBRARY_PATH\
' $s

##GENERATE Node Name from path
sed '/^NAME=/a \
if [ -z "$NAME" ]; then\
    NAME="pundun"`echo $HOSTNAME $RELEASE_ROOT_DIR | openssl sha1 | cut -b 10-15`\
    echo "Generated node name $NAME"\
    NAME_ARG="$NAME_TYPE $NAME"\
    sed -i -e "s;$NAME_TYPE;$NAME_TYPE $NAME;" $VMARGS_PATH\
fi\
' $s

## export PRODDIR
## gb_conf is using and dependent on PRODDIR env variable.
sed '/^RELEASE_ROOT_DIR=/a \
export PRODDIR=$RELEASE_ROOT_DIR\
' $s

done

## Create symlinks to configuration files
## This makes easier access to configuration files.
ETC=$REBAR_BUILD_DIR/rel/pundun/etc
CFGS=`find $REBAR_BUILD_DIR/rel/pundun/lib/*/priv/ -name *.yaml -type f`
for c in $CFGS; do
    ln -s $c $ETC/`basename $c`
done

# End
