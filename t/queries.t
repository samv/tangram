# -*- perl -*-
# (c) Sound Object Logic 2000-2001

use strict;
use lib "t";
use Springfield;

# $Tangram::TRACE = \*STDOUT;

use Test::More tests => 14;

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

is(&leaked, 0, "leaktest");

# BEGIN ks.perl@kurtstephens.com 2002/10/16
# Test non-commutative operator argument swapping
{
   my $storage = Springfield::connect;

   my ($person) = $storage->remote(qw( NaturalPerson ));
 
   #$DB::single = 1;
   # local $Tangram::TRACE = \*STDERR;
   my @results = $storage->select( $person,
      (1 <= $person->{person_id}) & ($person->{person_id} <= 2) );
   
   is(@results, 2, "non-commutative operator argument swapping" );

   $storage->disconnect();
}      

is(&leaked, 0, "leaktest");
# END ks.perl@kurtstephens.com 2002/10/16


# filter on string field

{
   my $storage = Springfield::connect;

   my ($person) = $storage->remote(qw( NaturalPerson ));

   my @results = $storage->select( $person, $person->{name} eq 'Simpson' );
   is(join( ' ', sort map { $_->{firstName} } @results ),
      'Homer Marge',
      "filter on string field");

   $storage->disconnect();
}      

is(&leaked, 0, "leaktest");

# logical and

{
   my $storage = Springfield::connect;

   my ($person) = $storage->remote(qw( NaturalPerson ));

   my @results = $storage->select( $person,
      $person->{firstName} eq 'Homer' & $person->{name} eq 'Simpson' );

   is( @results, 1, "Logical and");
   is ( $results[0]{firstName}, 'Homer', "Logical and" );

   $storage->disconnect();
}      

is(&leaked, 0, "leaktest");

{
   my $storage = Springfield::connect;

   my ($person, $partner) = $storage->remote(qw( NaturalPerson NaturalPerson ));

   my @results = $storage->select( $person,
      ($person->{partner} == $partner) & ($partner->{firstName} eq 'Marge') );

   is( @results, 1, "Logical and");
   is ( $results[0]{firstName}, 'Homer', "Logical and" );

   $storage->disconnect();
}      

is(&leaked, 0, "leaktest");

my $dbh = DBI->connect($cs, $user, $passwd)
    or die "DBI->connect failed; $DBI::errstr";

{
   my $storage = Springfield::connect(undef, { dbh => $dbh });

   my ($person) = $storage->remote(qw( NaturalPerson ));

   my @results = $storage->select( $person, $person->{partner} != undef );

   is(join( ' ', sort map { $_->{firstName} } @results ),
      'Homer Marge',
      "!= undef test");

   $storage->disconnect();
}

eval {
    my $sth = $dbh->prepare("select count(*) from Tangram")
	or die $DBI::errstr;

    $sth->execute();
    my @res = $sth->fetchall_arrayref;
};

is($@||$DBI::errstr||"", "",
   "Disconnect didn't disconnect a supplied DBI handle");

$dbh->disconnect();

is(&leaked, 0, "leaktest");
