#!/bin/sh
APOLLO_TAG="v1.0.1"
REPOSRC=https://github.com/erdemaksu/apollo.git
LOCALREPO=apollo

LOCALREPO_VC_DIR=$LOCALREPO/.git
cd $REBAR_DEPS_DIR

if [ ! -d $LOCALREPO_VC_DIR ]
then
    git clone $REPOSRC $LOCALREPO
else
    (cd $LOCALREPO && git pull $REPOSRC)
fi

(cd $LOCALREPO && git checkout $APOLLO_TAG 2>/dev/null || git checkout -b ?APOLLO_TAG)

cd $REBAR_ROOT_DIR

# Compile asn1 and output .erl and .hrl artifacts to src folder
erlc -o ./src/ $REBAR_DEPS_DIR/$LOCALREPO/*.asn1

# Create a symbolic link for the folder that includes proto files
MY_PROTO_LINK=config/proto
if [ -L $MY_PROTO_LINK ]; then
    rm $MY_PROTO_LINK
fi
ln -s $REBAR_DEPS_DIR/apollo $MY_PROTO_LINK

# Create a symbolic link so gpb plugin can access gpb.hrl
MY_LINK=include/gpb
if [ -L $MY_LINK ]; then
    rm $MY_LINK
fi
ln -s $REBAR_PLUGINS_DIR/gpb/include $MY_LINK
# End
