#!/usr/bin/perl

use FindBin '$Bin';

# goto WORK;

chdir '/tmp' or die;

system qq{tar xvfz $Bin/Tangram-1.19.tar.gz} unless -e 'Tangram-1.19';
chdir 'Tangram-1.19' or die;

$ENV{TANGRAM_CONFIG} = "$Bin/CONFIG.1.mysql";
system q{yes 2>/dev/null|perl Makefile.PL};

WORK:
chdir $Bin;

system qq{ perl -I $Bin/t.layout1 -I $Bin/t.layout1/t -I$Bin -e 'use Test::Harness qw(&runtests \$verbose); runtests \@ARGV;' t.layout1/t/*.t};
