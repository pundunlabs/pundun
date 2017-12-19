#!/bin/sh
OS_NAME=$(uname -s)
if [ $OS_NAME = "Darwin" ] ; then
    alias sed="sed -i '' "
else
    alias sed='sed -i '
fi

#Specify input scripts (Rebar3 provided and our modified)
START_SCRIPT=`find $REBAR_BUILD_DIR/rel/pundun/bin -type f -name pundun`
CUSTOM_SCRIPT=$REBAR_ROOT_DIR/config/pundun.start_script
#Specify output script file for merged content.
MERGED_SCRIPT=$REBAR_ROOT_DIR/config/merged.start_script
## Merge rebar3 script with our modified script.
## Take theirs (Rebar3 provided) in case of conflict.
diff -DVERSION1 $START_SCRIPT $CUSTOM_SCRIPT > $MERGED_SCRIPT
sed '/^#ifndef VERSION1/d' $MERGED_SCRIPT
sed '/^#else \/\* VERSION1 \*\//,/^#endif \/\* VERSION1 \*\//d' $MERGED_SCRIPT
sed '/^#ifdef VERSION1/d' $MERGED_SCRIPT
sed '/^#endif \/\* VERSION1 \*\//d' $MERGED_SCRIPT

## Find target script files to replace.
START_SCRIPTS=`find $REBAR_BUILD_DIR/rel/pundun/bin -type f -name 'pundun*'`
## Replace target script files with the merged script.
for s in $START_SCRIPTS; do
    cp $MERGED_SCRIPT $s
done
## Remove the merged script.
rm $MERGED_SCRIPT

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
	ln -sf ../${INSTALL_PATH}${c#${REBAR_BUILD_DIR}/rel/pundun/} $link
    done
}
create_sym_links

# End
