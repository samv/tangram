use strict;
use lib 't';
use Springfield;

Springfield::begin_tests(20);

# $Tangram::TRACE = \*STDOUT;

{
	my $storage = Springfield::connect_empty();

	my $homer = NaturalPerson->new( firstName => 'Homer',
									name => 'Simpson',
									interests => [ qw( beer food ) ] );

	$storage->insert($homer);

	$storage->disconnect();
}

leaktest();

{
	my $storage = Springfield::connect();
	my ($homer) = $storage->select('NaturalPerson');

	testcase("@{ $homer->{interests} }" eq 'beer food');

	$storage->disconnect();
}

leaktest();

{
	my $storage = Springfield::connect();
	my ($homer) = $storage->select('NaturalPerson');
	
	push @{ $homer->{interests} }, 'sex';
	$storage->update($homer);

	$storage->disconnect();
}

leaktest();

{
	my $storage = Springfield::connect();
	my ($homer) = $storage->select('NaturalPerson');

	testcase("@{ $homer->{interests} }" eq 'beer food sex');

	pop @{ $homer->{interests} };
	$storage->update($homer);

	$storage->disconnect();
}

leaktest();

{
	my $storage = Springfield::connect();
	my ($homer) = $storage->select('NaturalPerson');

	testcase("@{ $homer->{interests} }" eq 'beer food');
	
	unshift @{ $homer->{interests} }, 'sex';
	$storage->update($homer);

	$storage->disconnect();
}

leaktest();

{
	my $storage = Springfield::connect();
	my ($homer) = $storage->select('NaturalPerson');

	testcase("@{ $homer->{interests} }" eq 'sex beer food');
	
	delete $homer->{interests};
	$storage->update($homer);

	$storage->disconnect();
}

leaktest();

{
	my $storage = Springfield::connect();
	my ($homer) = $storage->select('NaturalPerson');

	testcase("@{ $homer->{interests} }" eq '');

	$homer->{interests} = [ qw( beer food ) ];
	$storage->update($homer);

	$storage->insert(
        NaturalPerson->new(
            firstName => 'Marge',
			name => 'Simpson',
			interests => [ qw( kids household ) ] ) );

	$storage->disconnect();
}

leaktest();

# exists, includes

{
	my $storage = Springfield::connect();

	my ($remote) = $storage->remote('NaturalPerson');
	my @results = $storage->select($remote, $remote->{interests}->includes('beer'));
	testcase(@results == 1 && $results[0]->{firstName} eq 'Homer');

	$storage->disconnect();
}

leaktest();
{
	my $storage = Springfield::connect();

	if ($Springfield::cs =~ /mysql/)
	{
		print STDERR "tests $Springfield::test (exists) skipped on this platform ";
		testcase(1);
	}
	else
	{

		my ($remote) = $storage->remote('NaturalPerson');
		my @results = $storage->select($remote, $remote->{interests}->exists('beer'));
		testcase(@results == 1 && $results[0]->{firstName} eq 'Homer');

		$storage->disconnect();
	}
}


leaktest();

# prefetch

{
	my $storage = Springfield::connect();

	my ($remote) = $storage->remote('NaturalPerson');
	$storage->prefetch($remote, 'interests');
	my ($homer) = $storage->select($remote, $remote->{firstName} eq 'Homer');

	{
		local ($storage->{db});
		testcase("@{ $homer->{interests} }" eq 'beer food');
	}

	$storage->disconnect();
}

leaktest();

{
	my $storage = Springfield::connect();

	my ($remote) = $storage->remote('NaturalPerson');
	$storage->prefetch($remote, 'interests', $remote->{firstName} eq 'Homer');

	my ($homer) = $storage->select($remote, $remote->{firstName} eq 'Homer');

	{
		local ($storage->{db});
		testcase("@{ $homer->{interests} }" eq 'beer food');
	}

	$storage->disconnect();
}

leaktest();

