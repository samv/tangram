use strict;
use t::Springfield;

# $Tangram::TRACE = \*STDOUT;

Springfield::begin_tests(3);

{
   my $storage = Springfield::connect_empty;

   $storage->insert
   (
      NaturalPerson->new( firstName => 'Homer', name => 'Simpson' ),
      NaturalPerson->new( firstName => 'Marge', name => 'Simpson' ),
      LegalPerson->new( name => 'Kwik Market' ),
      LegalPerson->new( name => 'Springfield Nuclear Power Plant' ),
   );

   $storage->disconnect;
}

Springfield::leaktest;


{
   my $storage = Springfield::connect;

   my $results = join( ', ', sort map { $_->as_string } $storage->select('Person') );
   #print "$results\n";

   Springfield::test( $results eq 'Homer Simpson, Kwik Market, Marge Simpson, Springfield Nuclear Power Plant' );
   $storage->disconnect;
}

Springfield::leaktest;
