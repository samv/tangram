use strict;
use lib 't';
use Springfield;

begin_tests(40);

package Vehicle;

sub new
  {
	my $self = bless { }, shift;
  }

sub make
  {
	my $class = shift;
	my $self = bless { }, $class;
	@$self{ $self->fields } = @_;
	return $self;
  }

sub state
  {
	my $self = shift;
	join ' ', ref($self), @$self{ $self->fields };
  }

package Boat;
use base qw( Vehicle );

sub fields { qw( name knots ) }

package Plane;
use base qw( Vehicle );

sub fields { qw( name altitude ) }

package HydroPlane;
use base qw( Boat Plane );

sub fields { qw( name knots altitude whatever ) }

sub check
  {
	my ($storage, $class, @states) = @_;
	my @objs = $storage->select($class);
	Springfield::test(@objs == @states);

	if (@objs == @states) {
	  my %states;
	  @states{ @states } = ();
	  delete @states{ map { $_->state } @objs };
	  Springfield::test(!keys %states);
	} else {
	  Springfield::test(0);
	}
  }

sub test_mapping
  {
	my ($v, $b, $p, $h) = @_;
	
	my $schema = Tangram::Relational
	  ->schema( {
				 control => 'Vehicles',

				 classes =>
				  [
				   Vehicle =>
				   {
					table => $v,
					abstract => 1,
					fields => { string => [ 'name' ] }
				   },
				   
				   Boat =>
				   {
					table => $b,
					bases => [ qw( Vehicle ) ],
					fields => { int => [ 'knots' ] },
				   },
				   
				   Plane =>
				   {
					table => $p,
					bases => [ qw( Vehicle ) ],
					fields => { int => [ 'altitude' ] },
				   },
				   
				   HydroPlane =>
				   {
					table => $h,
					bases => [ qw( Boat Plane ) ],
					fields => { string => [ 'whatever' ] },
				   },
				  ] } );

	my $dbh = DBI->connect($Springfield::cs, $Springfield::user, $Springfield::passwd, { PrintError => 0 });
	# $Tangram::TRACE = \*STDOUT;
	eval { $Springfield::dialect->retreat($schema, $dbh) };
	$Springfield::dialect->deploy($schema, $dbh);
	$dbh->disconnect();

	my $storage = Springfield::connect($schema);

	# use Data::Dumper;	print Dumper $storage->{engine}->get_polymorphic_select($schema->classdef('Boat'));	die;
	# my $t = HydroPlane->make(qw(Hydro 5 200 foo)); print Dumper $t; die;

	$storage->insert( Boat->make(qw( Erika 2 )), Plane->make(qw( AF-1 20000 )), HydroPlane->make(qw(Hydro 5 200 foo)) );

	check($storage, 'Boat', 'Boat Erika 2', 'HydroPlane Hydro 5 200 foo');
	check($storage, 'Plane', 'Plane AF-1 20000', 'HydroPlane Hydro 5 200 foo');
	check($storage, 'HydroPlane', 'HydroPlane Hydro 5 200 foo');
	check($storage, 'Vehicle', 'Boat Erika 2', 'Plane AF-1 20000', 'HydroPlane Hydro 5 200 foo');

	$storage->disconnect();
					 
  }

test_mapping('V', 'V', 'V', 'V');
test_mapping('V', 'V', 'V', 'H');
test_mapping('V', 'B', 'V', 'V');
test_mapping('V', 'V', 'P', 'V');
test_mapping('V', 'B', 'P', 'V');
