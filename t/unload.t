use strict;
use lib 't';
use Springfield;

# $Tangram::TRACE = \*STDOUT;

Springfield::begin_tests(3);

{
	my $storage = Springfield::connect_empty;

	$storage->insert( NaturalPerson->new( firstName => 'Homer' ));

	$storage->unload();

	Springfield::leaktest;

	$storage->disconnect();
}

{
	my $storage = Springfield::connect_empty;

	$storage->insert( my $homer = NaturalPerson->new( firstName => 'Homer' ));
	my $marge_id = $storage->insert( NaturalPerson->new( firstName => 'Marge' ));

	$storage->unload($homer);
	undef $homer;
	Springfield::test($SpringfieldObject::pop == 1);

	$storage->unload($marge_id);
	Springfield::leaktest;

	$storage->disconnect();
}
