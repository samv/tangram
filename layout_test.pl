#!/bin/bash

export TANGRAM_CONFIG=CONFIG.mysql
yes 2>/dev/null|perl Makefile.PL
make test

export TANGRAM_CONFIG=CONFIG.1.mysql;
perl -I. t.layout1/*.t
