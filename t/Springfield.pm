use strict;

use Tangram;

use Tangram::RawDate;
use Tangram::RawTime;
use Tangram::RawDateTime;

package Springfield;

use vars qw( $schema @ISA @EXPORT );

require Exporter;
@ISA = qw( Exporter );
@EXPORT = qw( optional_tests $schema );

$schema = Tangram::Schema->new( {

   #set_id => sub { my ($obj, $id) = @_; $obj->{id} = $id },
   #get_id => sub { shift()->{id} },

   sql =>
   {
	   cid_size => 2
   },
								  
   classes =>
   {
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
		 firstName => { sql => 'VARCHAR(40)' },
		 name => undef,
		},

		int => [ qw( age ) ],

		ref =>
		{
		 partner => undef,
		 credit => { aggreg => 1 },
		},

		#rawdate => [ qw( birthDate ) ],
		#rawtime => [ qw( birthTime ) ],
		rawdatetime => [ qw( birth ) ],

		array =>
		{
		 a_children =>
		 {
		  class => 'NaturalPerson',
		  table => 'a_children',
		  aggreg => 1,
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
	   manager => { null => 0 }
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
	  #int => { limit => { col => '_limit' } },
	  int => { limit => '_limit' },
	 }
	},

   } } );

use vars qw( $cs $user $passwd);

{
   local $/;

   my $config = $ENV{TANGRAM_CONFIG} || 'CONFIG';
   
   open CONFIG, $config
   or open CONFIG, "t/$config"
   or open CONFIG, "../t/$config"
   or die "Cannot open t/$config, reason: $!";

   ($cs, $user, $passwd) = split "\n", <CONFIG>;
}

my $no_tx;

sub connect
{
   my $storage = Tangram::Storage->connect($Springfield::schema, $cs, $user, $passwd) || die;
	$no_tx = $storage->{no_tx};
	return $storage;
}

sub connect_empty
{
   my $storage = Springfield::connect;
	my $conn = $storage->{db};

   foreach my $classdef (values %{ $Springfield::schema->{classes} })
   {
      $conn->do("DELETE FROM $classdef->{table}") or die
			unless $classdef->{stateless};
   }

   return $storage;
}

my $test;

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

	unless ($proceed)
	{
		print STDERR "tests $test-", $test + $tests - 1,
			" ($what) skipped on this platform ";
		test(1) while $tests--;
	}

	return $proceed;
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

use vars qw( @ISA );

@ISA = 'SpringfieldObject';

sub as_string
{
   die 'subclass responsibility';
}

#use overload '""' => sub { shift->as_string }, fallback => 1;

package NaturalPerson;

use vars qw( @ISA );

@ISA = 'Person';

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

use vars qw( @ISA );

@ISA = 'Person';

sub as_string
{
   return shift->{name};
}

package NuclearPlant;

use vars qw( @ISA );

@ISA = 'LegalPerson';

package Opinion;
use base qw( SpringfieldObject );

package Credit;
use base qw( SpringfieldObject );

1;
