use strict;

package SpringfieldObject;

sub new
  {
    my $class = shift;
    bless { @_ }, $class;
  }

package Person;
use base qw( SpringfieldObject );

package NaturalPerson;
use base qw( Person );

package LegalPerson;
use base qw( Person );

package Address;
use base qw( SpringfieldObject );

package Tangram::Springfield;
use Exporter;
use base qw( Exporter );
use vars qw( @EXPORT $schema );

@EXPORT = qw( $schema );

$schema = Tangram::Schema
  ->new(
	{
	 classes =>
	 {
	  Person =>
	  {
	   abstract => 1,

	   fields =>
	   {
	    iarray =>
	    {
	     addresses => { class => 'Address',
			  aggreg => 1 }
	    }
	   }
	  },

	  Address =>
	  {
	   fields =>
	   {
	    string => [ qw( type city ) ],
	   }
	  },

	  NaturalPerson =>
	  {
	   bases => [ qw( Person ) ],

	   fields =>
	   {
	    string   => [ qw( firstName name ) ],
	    int      => [ qw( age ) ],
	    ref      => [ qw( partner ) ],
	    array    => { children => 'NaturalPerson' },
	   },
	  },

	  LegalPerson =>
	  {
	   bases => [ qw( Person ) ],

	   fields =>
	   {
	    string   => [ qw( name ) ],
	    ref      => [ qw( manager ) ],
	   },
	  },
	 }
	} );

1;
