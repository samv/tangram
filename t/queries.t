# (c) Sound Object Logic 2000-2001

use strict;
use t::Springfield;

# $Tangram::TRACE = \*STDOUT;

Springfield::begin_tests(9);
                           
{
   my $storage = Springfield::connect_empty;

   my $homer = NaturalPerson->new( firstName => 'Homer', name => 'Simpson' );
   my $marge = NaturalPerson->new( firstName => 'Marge', name => 'Simpson' );

   $marge->{partner} = $homer;
   $homer->{partner} = $marge;

   $storage->insert( $homer );

   $storage->insert( NaturalPerson->new( firstName => 'Montgomery', name => 'Burns' ) );

   delete $homer->{partner};

   $storage->disconnect();
}

Springfield::leaktest;

# filter on string field

{
   my $storage = Springfield::connect;

   my ($person) = $storage->remote(qw( NaturalPerson ));

   my @results = $storage->select( $person, $person->{name} eq 'Simpson' );
   Springfield::test( join( ' ', sort map { $_->{firstName} } @results ) eq 'Homer Marge' );

   $storage->disconnect();
}      

Springfield::leaktest;

# logical and

{
   my $storage = Springfield::connect;

   my ($person) = $storage->remote(qw( NaturalPerson ));

   my @results = $storage->select( $person,
      $person->{firstName} eq 'Homer' & $person->{name} eq 'Simpson' );

   Springfield::test( @results == 1 && $results[0]{firstName} eq 'Homer' );

   $storage->disconnect();
}      

Springfield::leaktest;

{
   my $storage = Springfield::connect;

   my ($person, $partner) = $storage->remote(qw( NaturalPerson NaturalPerson ));

   my @results = $storage->select( $person,
      ($person->{partner} == $partner) & ($partner->{firstName} eq 'Marge') );

   Springfield::test( @results == 1 && $results[0]{firstName} eq 'Homer' );

   $storage->disconnect();
}      

Springfield::leaktest;

{
   my $storage = Springfield::connect;

   my ($person) = $storage->remote(qw( NaturalPerson ));

   my @results = $storage->select( $person, $person->{partner} != undef );
   print join(' ', map { $_->{firstName} } @results), "\n";

   Springfield::test(
      join( ' ', sort map { $_->{firstName} } @results ) eq 'Homer Marge' );

   $storage->disconnect();
}      

Springfield::leaktest;
