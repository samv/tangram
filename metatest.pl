#!/bin/bash

for CFG in mysql oracle sybase pg;
do {
	export TANGRAM_CONFIG=CONFIG.$CFG ;
	yes 2>/dev/null|perl Makefile.PL ;
	make test ;
} done