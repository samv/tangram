use strict;

use Tangram;

package Springfield;

use vars qw( $schema );

$schema = Tangram::Schema->new( {

   #set_id => sub { my ($obj, $id) = @_; $obj->{id} = $id },
   #get_id => sub { shift()->{id} },

   classes =>
   {
      Person =>
      {
         abstract => 1,
      },

      NaturalPerson =>
      {
         bases =>
            [ qw( Person ) ],

         members =>
         {
            string =>
	    {
		firstName => { sql => 'VARCHAR(40)' },
		name => undef,
	    },

            int =>
               [ qw( age ) ],

            ref =>
               [ qw( partner ) ],

            array =>
            {
               a_children =>
               {
                  class => 'NaturalPerson',
                  table => 'a_children',
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
                  back => 'ia_parent'
               }
            },

            set =>
            {
               s_children =>
               {
                  class => 'NaturalPerson',
                  table => 's_children',
               }
            },

            iset =>
            {
               is_children =>
               {
                  class => 'NaturalPerson',
                  coll => 'is_ref',
                  slot => 'is_slot',
                  back => 'is_parent'
               }
            },
         },
      },

      Opinion =>
      {
         members =>
         {
            string =>
               [ qw( statement ) ],
         },
      },

      LegalPerson =>
      {
         bases =>
            [ qw( Person ) ],

         members =>
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

         members =>
         {
            int => [ qw( curies ) ],
         },
      },
   
      NuclearPlant =>
      {
         bases => [ qw( LegalPerson EcologicalRisk ) ],

         members =>
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

   } } );

my ($cs, $user, $passwd);

{
   local $/;
   
   open CONFIG, 'CONFIG'
   or open CONFIG, 't/CONFIG'
   or open CONFIG, '../t/CONFIG'
   or die "Cannot open 't/CONFIG', reason: $!";

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

1;
