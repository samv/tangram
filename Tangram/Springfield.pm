use strict;

package Person;

sub new
  {
    my $class = shift;
    bless { @_ }, $class;
  }

package NaturalPerson;
use base qw( Person );

package LegalPerson;
use base qw( Person );

package main;

use vars qw( $schema );

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
