# -*- perl -*-
# (c) Sound Object Logic 2000-2001

use strict;
use t::Springfield;

use vars qw( $intrusive );

my $children = $intrusive ? 'is_children' : 's_children';
my %id;
my @kids = qw( Bart Lisa Maggie );

sub NaturalPerson::children
{
	my ($self) = @_;
	my @children = sort { $a->{firstName} cmp $b->{firstName} }
		$self->{$children}->members;
	return wantarray ? @children : join(' ', map { $_->{firstName} } @children );
}

sub marge_test
{
	my $storage = shift;
	Springfield::test( $intrusive
					   || $storage->load( $id{Marge} )->children eq 'Bart Lisa Maggie' );
}

sub stdpop
{
	my $storage = Springfield::connect_empty;

	my @children = map { NaturalPerson->new( firstName => $_ ) } @kids;
	@id{ @kids } = $storage->insert( @children );
   
	my $homer = NaturalPerson->new( firstName => 'Homer',
									$children => Set::Object->new( @children ) );
	$id{Homer} = $storage->insert($homer);
   
	my $marge = NaturalPerson->new( firstName => 'Marge' );
	$marge->{$children} = Set::Object->new( @children ) unless $intrusive;
	$id{Marge} = $storage->insert($marge);

	$storage->disconnect;
}

Springfield::begin_tests(42);

stdpop();

Springfield::leaktest;

{
	my $storage = Springfield::connect;
	my $homer = $storage->load( $id{Homer} );

	Springfield::test( $homer->children eq 'Bart Lisa Maggie' );
	marge_test( $storage );

	$homer->{$children}->remove( $storage->load( $id{Bart} ) );
	$storage->update( $homer );

	$storage->disconnect;
}

###############################################
# insert

{
	my $storage = Springfield::connect;

	my $homer = $storage->load($id{Homer}) or die;

	Springfield::test( $homer->children eq 'Lisa Maggie' );
	marge_test( $storage );

	$homer->{$children}->clear();
	$storage->update($homer);

	$storage->disconnect;
}

Springfield::leaktest;

{
	my $storage = Springfield::connect;
	my $homer = $storage->load($id{Homer}) or die;

	Springfield::test( $homer->children eq '' );
	marge_test( $storage );

	$homer->{$children}->insert( $storage->load( $id{Bart} ) );
	$storage->update($homer);

	$storage->disconnect;
}

Springfield::leaktest;

{
	my $storage = Springfield::connect;
	my $homer = $storage->load($id{Homer}) or die;

	Springfield::test( $homer->children eq 'Bart' );
	marge_test( $storage );

	$homer->{$children}->insert( $storage->load( @id{qw( Lisa Maggie )} ) );
	$storage->update($homer);

	$storage->disconnect;
}

Springfield::leaktest;

{
	my $storage = Springfield::connect;
	my $homer = $storage->load( $id{Homer} );

	Springfield::test( $homer->children eq 'Bart Lisa Maggie' );
	marge_test( $storage );

	$storage->disconnect;
}

Springfield::leaktest;

{
	my $storage = Springfield::connect;
	my $homer = $storage->load( $id{Homer} );

	Springfield::test( $homer->children eq 'Bart Lisa Maggie' );
	marge_test( $storage );

	$storage->reset();
	undef $homer;

	Springfield::leaktest;

	$storage->disconnect;
}

{
	my $storage = Springfield::connect;

	my @prefetch = $storage->prefetch( 'NaturalPerson', $children );

	my $homer = $storage->load( $id{Homer} );

	Springfield::test( $homer->children eq 'Bart Lisa Maggie' );
	marge_test( $storage );

	$storage->disconnect;
}

Springfield::leaktest;

{
	my $storage = Springfield::connect;
	my $bart = $storage->load( $id{Bart} );

	Springfield::test( !$intrusive || $bart->{is_parent}{firstName} eq 'Homer' );
	marge_test( $storage );

	$storage->disconnect;
}

Springfield::leaktest;

{
	my $storage = Springfield::connect;

	my $person = $storage->remote('NaturalPerson');

	my @prefetch = $storage->prefetch( $person, $children, $person->{firstName} eq 'Homer' );

	my $homer = $storage->load( $id{Homer} );

	Springfield::test( $homer->children eq 'Bart Lisa Maggie' );
	marge_test( $storage );

	$storage->disconnect;
}

Springfield::leaktest;

#########
# queries

my $parents = $intrusive ? 'Homer' : 'Homer Marge';

{
	my $storage = Springfield::connect;
	my ($parent, $child) = $storage->remote(qw( NaturalPerson NaturalPerson ));

	#local $Opal::TRACE = \*STDOUT;

	my @results = $storage->select( $parent, $parent->{$children}->includes( $child )
									& $child->{firstName} eq 'Bart' );

	Springfield::test( join( ' ', sort map { $_->{firstName} } @results ) eq $parents );
   
	$storage->disconnect();
}      

Springfield::leaktest;

{
	my $storage = Springfield::connect;
	my $parent = $storage->remote( 'NaturalPerson' );
	my $bart = $storage->load( $id{Bart} );

	my @results = $storage->select( $parent, $parent->{$children}->includes( $bart ) );

	Springfield::test( join( ' ', sort map { $_->{firstName} } @results ) eq $parents );
	$storage->disconnect();
}      

Springfield::leaktest;

{
	my $storage = Springfield::connect_empty;

	my @children = map { NaturalPerson->new( firstName => $_ ) } @kids;
   
	my $homer = NaturalPerson->new( firstName => 'Homer',
	    $children => Set::Object->new( @children ) );

	my $abe = NaturalPerson->new( firstName => 'Abe',
        $children => Set::Object->new( $homer ) );

	$id{Abe} = $storage->insert($abe);

	$storage->disconnect();
}      

Springfield::leaktest;

{
	my $storage = Springfield::connect;

	$storage->erase( $storage->load( $id{Abe} ) );

	my @pop = $storage->select('NaturalPerson');
	Springfield::test( @pop == 0 );

	$storage->disconnect();
}      

Springfield::leaktest;

#############################################################################
# Tx

Springfield::tx_tests(8,
    sub
    {

		stdpop();

		# check rollback of DB tx

		Springfield::leaktest;
		{
			my $storage = Springfield::connect;
			my $homer = $storage->load( $id{Homer} );

			$storage->tx_start();

			$homer->{$children}->remove( $storage->load( $id{Bart} ) );
			$storage->update( $homer );

			$storage->tx_rollback();

			$storage->disconnect;
		}

		Springfield::leaktest;

		# storage should still contain 3 children

		{
			my $storage = Springfield::connect;
			my $homer = $storage->load( $id{Homer} );

			Springfield::test( $homer->children eq 'Bart Lisa Maggie' );
			marge_test( $storage );

			$storage->disconnect;
		}

		Springfield::leaktest;

		# check that DB and collection state remain in synch in case of rollback

		{
			my $storage = Springfield::connect;
			my $homer = $storage->load( $id{Homer} );

			$storage->tx_start();

			$homer->{$children}->remove( $storage->load( $id{Bart} ) );
			$storage->update( $homer );

			$storage->tx_rollback();

			$storage->update( $homer );

			$storage->disconnect;
		}

		# Bart should no longer be Homer's child

		{
			my $storage = Springfield::connect;
			my $homer = $storage->load( $id{Homer} );

			Springfield::test( $homer->children eq 'Lisa Maggie' );
			marge_test( $storage );

			$storage->disconnect;
		}

		Springfield::leaktest;

	} ); # tx_tests

1;
