#!/usr/bin/perl -w

use strict;

delete $ENV{LANG};

my %target;
@target{@ARGV} = ();

for my $CFG (qw( mysql sybase pg )) {
  next if @ARGV && !exists $target{$CFG};
  $ENV{TANGRAM_CONFIG} = "CONFIG.$CFG";
  system 'yes 2>/dev/null|perl Makefile.PL';
  system 'make test';
}

if (!@ARGV || exists $target{layout1}) {
  system 'layout_test.pl';
}
