use strict;

use Tangram;

use Tangram::RawDate;
use Tangram::RawTime;
use Tangram::RawDateTime;
use Tangram::DMDateTime;

use Tangram::FlatArray;
use Tangram::FlatHash;
use Tangram::PerlDump;

package Springfield;
use Exporter;
use vars qw(@ISA @EXPORT @EXPORT_OK);
@ISA = qw( Exporter );

@EXPORT = qw( &optional_tests $schema testcase &leaktest &test &begin_tests &tests_for_dialect $dialect $cs $user $passwd );
@EXPORT_OK = @EXPORT;

use vars qw($cs $user $passwd $dialect $vendor $schema);

{
  local $/;
  
  my $config = $ENV{TANGRAM_CONFIG} || 'CONFIG';
  
  open CONFIG, $config
	or open CONFIG, "t/$config"
	  or open CONFIG, "../t/$config"
		or die "Cannot open t/$config, reason: $!";
  
  ($cs, $user, $passwd) = split "\n", <CONFIG>;
  
  $vendor = (split ':', $cs)[1];;
  $dialect = "Tangram::$vendor";  # deduce dialect from DBI driver
  eval "use $dialect";
  $dialect = 'Tangram::Relational' if $@;
}

sub list_if {
  shift() ? @_ : ()
}

my $no_tx;

$schema = Tangram::Schema->new( {

   #set_id => sub { my ($obj, $id) = @_; $obj->{id} = $id },
   #get_id => sub { shift()->{id} },

   sql =>
   {
	   cid_size => 3,
   },

   class_table => 'Classes',
								  
   classes =>
   [
      Person =>
      {
         abstract => 1,
      },

      NaturalPerson =>
      {
	   table => 'NP',

	   bases => [ qw( Person ) ],

	   fields =>
	   {
		string =>
		{
		 firstName => undef,
		 name => undef,
		},

		int => [ qw( age ) ],

		ref =>
		{
		 partner => undef,
		 credit => { aggreg => 1 },
		},

		list_if( $vendor ne 'Sybase',
				 rawdate => [ qw( birthDate ) ],
				 rawtime => [ qw( birthTime ) ],
				 rawdatetime => [ qw( birth ) ],
				 dmdatetime => [ qw( incarnation ) ]
			   ),

		array =>
		{
		 children =>
		 {
		  class => 'NaturalPerson',
		  table => 'a_children',
		  aggreg => 1,
		 },
		 belongings =>
		 {
		  class => 'Item',
		  aggreg => 1,
		  deep_update => 1
		 }
		},

		hash =>
		{
		 h_opinions =>
		 {
		  class => 'Opinion',
		  table => 'h_opinions',
		 }
		},

		iarray =>
		{
		 ia_children =>
		 {
		  class => 'NaturalPerson',
		  coll => 'ia_ref',
		  slot => 'ia_slot',
		  back => 'ia_parent',
		  aggreg => 1,
		 }
		},

		set =>
		{
		 s_children =>
		 {
		  class => 'NaturalPerson',
		  table => 's_children',
		  aggreg => 1,
		 }
		},

		iset =>
		{
		 is_children =>
		 {
		  class => 'NaturalPerson',
		  coll => 'is_ref',
		  slot => 'is_slot',
		  back => 'is_parent',
		  aggreg => 1,
		 }
		},

		flat_array => [ qw( interests ) ],

		flat_hash => [ qw( opinions ) ],

		perl_dump => [ qw( brains ) ],
	   },
      },

	Opinion =>
	{
	 fields =>
	 {
	  string => [ qw( statement ) ],
	 },
	},

	LegalPerson =>
	{
	 bases => [ qw( Person ) ],

	 fields =>
	 {
	  string =>
	  [ qw( name ) ],

	  ref =>
	  {		
	   manager => { null => 1 }
	  },
	 },
	},

	EcologicalRisk =>
	{
	 abstract => 1,

	 fields =>
	 {
	  int => [ qw( curies ) ],
	 },
	},
   
	NuclearPlant =>
	{
	 bases => [ qw( LegalPerson EcologicalRisk ) ],

	 fields =>
	 {
	  array =>
	  {
	   employees =>
	   {
		class => 'NaturalPerson',
		table => 'employees'
	   }
	  },
	 },
	},

	Credit =>
	{
	 fields =>
	 {
	  #int => { limit => { col => 'theLimit' } },
	  int => { limit => 'theLimit' },
	 }
	},

        Item =>
        {
	 fields =>
	 {
	  string => [ qw(name) ],
	  ref =>
	  {
	   owner => { deep_update => 1 }
	  }
	 }
	},

   ] } );

sub connect
  {
	my $schema = shift || $Springfield::schema;
	my $opts = {};
	$opts->{no_tx} = 1 if $cs =~ /^dbi:mysql:/;
	my $storage = $dialect->connect($schema, $cs, $user, $passwd, $opts) || die;
	$no_tx = $storage->{no_tx};
	return $storage;
  }

sub empty
  {
	my $storage = shift || Springfield::connect;
	my $schema = shift || $Springfield::schema;
	my $conn = $storage->{db};

	foreach my $classdef (values %{ $schema->{classes} }) {
      $conn->do("DELETE FROM $classdef->{table}") or die
		unless $classdef->{stateless};
	}

	$conn->do('DELETE FROM a_children');
	$conn->do('DELETE FROM s_children');
  }

sub connect_empty
  {
	my $schema = shift || $Springfield::schema;
	my $storage = Springfield::connect($schema);
	empty($storage, $schema);
	return $storage;
  }

use vars qw( $test );

sub begin_tests
{
   print "1..", shift, "\n";
   $test = 1;
}

sub test
{
	my $ok = shift;
   print 'not ' unless $ok;
   print 'ok ', $test++;
	print "\n";

	my ($fun, $file, $line) = caller;
	print "$file($line) : error\n" unless $ok;
}

*testcase = \&test;

sub leaktest
{
   if ($SpringfieldObject::pop == 0)
   {
      print "ok $test\n";
   }
   else
   {
		my ($fun, $file, $line) = caller;
      print "not ok $test\n";
		print "$file($line) : error: $SpringfieldObject::pop object(s) leaked\n";
   }

   $SpringfieldObject::pop = 0;

   ++$test;
}

sub tx_tests
{
	my ($tests, $code) = @_;

	if ($no_tx)
	{
		print STDERR "tests $test-", $test + $tests - 1, " (transactions) skipped on this platform ";
		test(1) while $tests--;
	}
	else
	{
		&$code;
	}
}

sub optional_tests
{
	my ($what, $proceed, $tests) = @_;

	$test ||= 1;

	unless ($proceed)
	{
		print STDERR "tests $test-", $test + $tests - 1,
			" ($what) skipped on this platform ";
		test(1) while $tests--;
	}

	return $proceed;
}

sub tests_for_dialect {
	my %dialect;
	@dialect{@_} = ();
	return if exists $dialect{ (split ':', $cs)[1] };

	begin_tests(1);
	optional_tests($dialect, 0, 1);
	exit;
  }

#use Data::Dumper;
#print Dumper $schema;
#deploy;

package SpringfieldObject;

use vars qw( $pop );

sub new
{
   my $pkg = shift;
   ++$pop;
   return bless { $pkg->defaults, @_ }, $pkg;
}

sub defaults
{
   return ();
}

sub DESTROY
{
#   die if exists shift->{id};
   --$pop;
}

package Person;
use vars qw(@ISA);
@ISA = qw( SpringfieldObject );

sub as_string
{
   die 'subclass responsibility';
}

#use overload '""' => sub { shift->as_string }, fallback => 1;

package NaturalPerson;
use vars qw(@ISA);
@ISA = qw( Person );

sub defaults
{
   a_children => [], ia_children => [],
	s_children => Set::Object->new, is_children => Set::Object->new,
   h_opinions => {}
}

sub as_string
{
   my ($self) = @_;
	local $^W; # why? get use of undefined value otherwise
   exists($self->{name}) && exists($self->{firstName}) && "$self->{firstName} $self->{name}"
	|| $self->{firstName} || $self->{name}
}

package LegalPerson;

use vars qw(@ISA);
@ISA = 'Person';

sub as_string
{
   return shift->{name};
}

package NuclearPlant;
use vars qw(@ISA);
@ISA = qw( LegalPerson );

package Opinion;
use vars qw(@ISA);
@ISA = qw( SpringfieldObject );

package Credit;
use vars qw(@ISA);
@ISA = qw( SpringfieldObject );

package Item;
use vars qw(@ISA);
@ISA = qw( SpringfieldObject );

1;
