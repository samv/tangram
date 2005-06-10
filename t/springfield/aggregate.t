#!/usr/bin/perl -w

use strict;
use Test::More tests => 7;
use lib "t/springfield";
use Springfield;

=head1 NAME

t/aggregate.t - test aggregate tangram functions

=head1 SYNOPSIS

 perl -Mlib=. t/aggregate.t

=head1 DESCRIPTION

This test script tests using Tangram for aggregate functionality, such
as when no object is selected.

=cut

stdpop();

my $dbh = DBI->connect($cs, $user, $passwd)
    or die "DBI->connect failed; $DBI::errstr";

# test GROUP BY and COUNT
{
   my $storage = Springfield::connect(undef, { dbh => $dbh });
   my ($r_person, $r_child) = $storage->remote(("NaturalPerson")x2);

   #local($Tangram::TRACE)=\*STDERR;
   my $cursor = $storage->cursor
       ( undef,
	 filter => $r_person->{children}->includes($r_child),
	 group => [ $r_person ],
	 retrieve => [ $r_child->{id}->count(), $r_child->{age}->sum() ],
	 #order => [ $r_child->{id}->count() ],
       );

   my @data;
   while ( my $row = $cursor->current() ) {
       push @data, [ $cursor->residue ];
       $cursor->next();
   }
   @data = sort { $a->[0] <=> $b->[0] } @data;
   #print Data::Dumper::Dumper(\@data);
   is_deeply(\@data, [ [ 1, 38 ], [3, 19 ], [3, 19] ],
	     "GROUP BY, SUM(), COUNT()");
}
is(&leaked, 0, "leaktest");

# test $storage->sum() - single, array ref, set arguments
{
    my $storage = Springfield::connect(undef, { dbh => $dbh});
    my ($r_person) = $storage->remote("NaturalPerson");

    is($storage->sum($r_person->{age}), 156,
       "Tangram::Storage->sum()");

    is(&leaked, 0, "leaktest");
}
