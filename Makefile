export

REPO	    ?= pundun
PKG_REVISION    ?= $(shell git describe --tags)
GIT_TAG ?= $(shell git describe --tags | tr - .)
PKG_VERSION ?= $(GIT_TAG:v%=%)
PKG_ID           = pundun-$(PKG_VERSION)
PKG_BUILD        = 1
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR           ?= $(shell which rebar)
OVERLAY_VARS    ?=
DEPS_DIR	= ${BASE_DIR}/_build/default/lib
REL_DIR		= ${BASE_DIR}/_build/target/rel

.PHONY: rel deps clean

all: deps compile

compile: deps
	@(rebar3 compile)

deps:
	@rebar3 compile

rel:
	@rebar3 as target release

clean:
	@rebar3 clean

rel_clean:
	rm -rf ${REL_DIR}/pundun

.PHONY: package

package.src:
	rm -rf package
	mkdir -p package/$(PKG_ID)
	git archive --format=tar --prefix=$(PKG_ID)/ $(PKG_REVISION)| (cd package && tar -xf -)
	${MAKE} -C package/$(PKG_ID) deps
	tar -C package -czf package/$(PKG_ID).tar.gz $(PKG_ID)

package: package.src
	${MAKE} -C package -f ${DEPS_DIR}/node_package/Makefile

package_clean:
	rm -rf package/pundun_* package/pundun-${PKG_VERSION}
