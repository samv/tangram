# (c) Sound Object Logic 2000-2001

use strict;
use lib 't';
use Springfield;
use Data::Dumper;

Springfield::begin_tests(3);

#$Tangram::TRACE = \*STDOUT;

sub graph {
  NaturalPerson->new( firstName => 'Homer',
					  name => 'Simpson',
					  h_opinions => { work => Opinion->new(statement => 'bad'),
									  food => Opinion->new(statement => 'good'),
									  beer => Opinion->new(statement => 'better') } );
}

{
	my $storage = Springfield::connect_empty();

	my $homer = graph();

	$storage->insert($homer);

	$storage->disconnect();
}

leaktest();

{
	my $storage = Springfield::connect();
	my ($homer) = $storage->select('NaturalPerson');

	testcase(Dumper($homer->{h_opinions}) eq Dumper(graph()->{h_opinions}));

	$storage->disconnect();
}

leaktest();
