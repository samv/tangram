use strict;
use t::Springfield;

use vars qw( $intrusive );

my $children = $intrusive ? 'ia_children' : 'a_children';

my %id;
my @kids = qw( Bart Lisa Maggie );

# $Tangram::TRACE = \*STDOUT;   

sub NaturalPerson::children
{
   my ($self) = @_;
   join(' ', map { $_->{firstName} } @{ $self->{$children} } )
}

sub marge_test
{
	my $storage = shift;
	Springfield::test( $intrusive
		|| $storage->load( $id{Marge} )->children eq 'Bart Lisa Maggie' );
}

Springfield::begin_tests(42);

sub stdpop
{
   my $storage = Springfield::connect_empty;

	my @children = map { NaturalPerson->new( firstName => $_ ) } @kids;
	@id{ @kids } = $storage->insert( @children );
   
	my $homer = NaturalPerson->new( firstName => 'Homer', $children => [ @children ] );
   $id{Homer} = $storage->insert($homer);
   
	my $marge = NaturalPerson->new( firstName => 'Marge' );
   $marge->{$children} = [ @children ] unless $intrusive;
	$id{Marge} = $storage->insert($marge);

   $storage->disconnect;
}

#############################################################################

stdpop();

Springfield::leaktest;

{
   my $storage = Springfield::connect;
   my $homer = $storage->load( $id{Homer} );

   Springfield::test( $homer->children eq 'Bart Lisa Maggie' );
	marge_test( $storage );

	@{ $homer->{$children} }[0, 2] = @{ $homer->{$children} }[2, 0];
	$storage->update( $homer );

   $storage->disconnect;
}

Springfield::leaktest;

{
   my $storage = Springfield::connect;
   my $homer = $storage->load( $id{Homer} );

   Springfield::test( $homer->children eq 'Maggie Lisa Bart' );
	marge_test( $storage );

	pop @{ $homer->{$children} };
	$storage->update( $homer );

   $storage->disconnect;
}

###############################################
# insert

{
   my $storage = Springfield::connect;

   my $homer = $storage->load($id{Homer}) or die;

   Springfield::test( $homer->children eq 'Maggie Lisa' );

	shift @{ $homer->{$children} };
	$storage->update($homer);

   $storage->disconnect;
}

Springfield::leaktest;

{
   my $storage = Springfield::connect;
   my $homer = $storage->load($id{Homer}) or die;
   Springfield::test( $homer->children eq 'Lisa' );
   $storage->disconnect;
}

Springfield::leaktest;

{
   my $storage = Springfield::connect;
   my $homer = $storage->load($id{Homer}) or die;
   shift @{ $homer->{$children} };
   $storage->update($homer);
   $storage->disconnect;
}

Springfield::leaktest;

{
   my $storage = Springfield::connect;
   my $homer = $storage->load($id{Homer}) or die;

   Springfield::test( $homer->children eq '' );

	push @{ $homer->{$children} }, $storage->load( $id{Bart} );
   $storage->update($homer);

   $storage->disconnect;
}

Springfield::leaktest;

{
   my $storage = Springfield::connect;
   my $homer = $storage->load($id{Homer}) or die;

   Springfield::test( $homer->children eq 'Bart' );

	push @{ $homer->{$children} }, $storage->load( @id{qw( Lisa Maggie )} );
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
   my $bart = $storage->load( $id{Bart} );

   Springfield::test( !$intrusive || $bart->{ia_parent}{firstName} eq 'Homer' );
	marge_test( $storage );

   $storage->disconnect;
}

Springfield::leaktest;

##########
# prefetch

{
   my $storage = Springfield::connect;

	my @prefetch = $storage->prefetch( 'NaturalPerson', $children );

   my $homer = $storage->load( $id{Homer} );

   Springfield::test( $homer->children eq 'Bart Lisa Maggie' );
	marge_test( $storage );

   $storage->disconnect();
}      

Springfield::leaktest;

{
   my $storage = Springfield::connect;

   my $person = $storage->remote('NaturalPerson');

	my @prefetch = $storage->prefetch( 'NaturalPerson', $children );

   my $homer = $storage->load( $id{Homer} );

   Springfield::test( $homer->children eq 'Bart Lisa Maggie' );
	marge_test( $storage );

   $storage->disconnect();
}      

Springfield::leaktest;

#########
# queries

my $parents = $intrusive ? 'Homer' : 'Homer Marge';

{
   my $storage = Springfield::connect;
   my ($parent, $child) = $storage->remote(qw( NaturalPerson NaturalPerson ));

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
		children => [ map { NaturalPerson->new( firstName => $_ ) } @kids ] );

	my $abe = NaturalPerson->new( firstName => 'Abe',
        $children => [ $homer ] );

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

Springfield::tx_tests(8, sub {

stdpop();

# check rollback of DB tx

Springfield::leaktest;
{
   my $storage = Springfield::connect;
   my $homer = $storage->load( $id{Homer} );

   $storage->tx_start();

	shift @{ $homer->{$children} };
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

	shift @{ $homer->{$children} };
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