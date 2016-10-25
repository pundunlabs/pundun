#!/bin/sh

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

cd $REBAR_ROOT_DIR

erlc -o ./src/ $REBAR_DEPS_DIR/$LOCALREPO/*.asn1
# End
