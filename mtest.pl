#!/usr/bin/perl -w

use strict;

@ARGV = qw( mysql pg ) unless @ARGV;

delete $ENV{LANG};

for my $CFG ( @ARGV ) {
  $ENV{TANGRAM_CONFIG} = "CONFIG.$CFG";
  system 'yes 2>/dev/null|perl Makefile.PL';
  system 'make test';
}

# if (!@ARGV || exists $target{layout1}) {
#   system 'layout_test.pl';
# }
