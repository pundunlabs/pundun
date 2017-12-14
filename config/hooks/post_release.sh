#!/bin/sh

START_SCRIPTS=`find $REBAR_BUILD_DIR/rel/pundun/bin -type f -name 'pundun*'`
for s in $START_SCRIPTS; do
    cp $REBAR_ROOT_DIR/config/pundun.start_script $s
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
	ln -sf ../${INSTALL_PATH}${c#${REBAR_BUILD_DIR}/rel/pundun/} $link
    done
}
create_sym_links

# End
