#  -*- perl -*-
# (c) Sound Object Logic 2000-2001

use strict;
use lib 't';
use Springfield;
use Test::More tests => 3;

#$Tangram::TRACE = \*STDOUT;

sub graph {
    NaturalPerson->new( firstName => 'Homer',
			name => 'Simpson',
			h_opinions =>
			{ work => Opinion->new(statement => 'bad'),
			  food => Opinion->new(statement => 'good'),
			  beer => Opinion->new(statement => 'better') } );
}

{
	my $storage = Springfield::connect_empty();

	my $homer = graph();

	$storage->insert($homer);

	$storage->disconnect();
}

is(leaked, 0, "leaktest");

{
	my $storage = Springfield::connect();
	my ($homer) = $storage->select('NaturalPerson');

	is_deeply($homer->{h_opinions}, graph()->{h_opinions},
		  "Hash returned intact");

	$storage->disconnect();
}

is(leaked, 0, "leaktest");
