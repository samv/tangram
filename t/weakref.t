use strict;
use lib 't';
use Springfield;

# $Tangram::TRACE = \*STDOUT;

my $tests = 3;
Springfield::begin_tests($tests);

Springfield::optional_tests('weakrefs', !$Tangram::no_weakrefs, $tests)
  or exit;

{
  my $storage = Springfield::connect_empty;

  $storage->insert( NaturalPerson->new( firstName => 'Homer' ));

  Springfield::leaktest;

  $storage->disconnect();
}

{
  my $storage = Springfield::connect;

  {
    my ($homer) = $storage->select('Person');
    Springfield::test($SpringfieldObject::pop == 1);
  }

  Springfield::leaktest;

  $storage->disconnect();
}
