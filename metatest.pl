#!/usr/bin/perl -w

use strict;

# note to the observant - this list represents the lists of RDBMSes
# the maintainer is hopefully testing releases on :-)
@ARGV = qw( SQLite SQLite2 myisam innodb Pg ) unless @ARGV;

delete $ENV{LANG};

for my $CFG ( @ARGV ) {
  $ENV{TANGRAM_CONFIG} = "t/CONFIG.$CFG";
  system 'yes 2>/dev/null|perl Makefile.PL';
  system 'make test';
}

# if (!@ARGV || exists $target{layout1}) {
#   system 'layout_test.pl';
# }
