#!/bin/bash

export LANG=''

for CFG in mysql sybase pg;
do {
	export TANGRAM_CONFIG=CONFIG.$CFG ;
	yes 2>/dev/null|perl Makefile.PL ;
	make test ;
} done

export TANGRAM_CONFIG=CONFIG.1.mysql;
yes 2>/dev/null|perl Makefile.PL
make test
