# -*- perl -*-
# (c) Sound Object Logic 2000-2001

use strict;
use lib 't';
use Springfield;
BEGIN {
    eval "use Scalar::Util";
    eval "use WeakRef" if $@;
    if ($@) {
	eval 'use Test::More skip_all => "No WeakRef / Scalar::Util"';
	exit;
    } else {
	eval 'use Test::More tests => 3;';
    }
}

# $Tangram::TRACE = \*STDOUT;

my $tests = 3;

{
  my $storage = Springfield::connect_empty;

  $storage->insert( NaturalPerson->new( firstName => 'Homer' ));

  is(leaked, 0, "WeakRef works");

  $storage->disconnect();
}

{
  my $storage = Springfield::connect;

  {
    my ($homer) = $storage->select('Person');
    is($SpringfieldObject::pop, 1,
       "Objects not lost until they fall out of scope");
  }

  is(leaked, 0, "WeakRef still works");

  $storage->disconnect();
}
