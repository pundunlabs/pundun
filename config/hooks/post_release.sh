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
    sed 's/readlink/readlink -f/g' $s

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

#Initialize erlang hosts file with localhost.
sed '/^export PRODDIR=/i \
\
if [ ! -f $ROOTDIR/.hosts.erlang ]; then\
    echo '"\\\'"'`hostname`'"\\\'"'. >> $ROOTDIR/.hosts.erlang\
fi\
' $s

done

## Create symlinks to configuration files
## This makes easier access to configuration files.
create_sym_links()
{
    CFGS=`find $REBAR_BUILD_DIR/rel/pundun/lib/*/priv/ -name *.yaml -type f`
    if [ "$PKG_BUILD" = "1" ] ; then
	INSTALL_PATH="../usr/lib/pundun/"
    else
	INSTALL_PATH=""
    fi
    for c in $CFGS; do
	local link=$REBAR_BUILD_DIR/rel/pundun/etc/`basename $c`
	if [ -L $link ]; then
	    rm $c
	fi
	ln -sf ../${INSTALL_PATH}${c#${REBAR_BUILD_DIR}/rel/pundun/} $link
    done
}
create_sym_links
# End
