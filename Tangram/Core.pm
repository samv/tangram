use strict;

use Set::Object;

use Tangram::Scalar;
use Tangram::Ref;

use Tangram::Schema;
use Tangram::Cursor;
use Tangram::Storage;
use Tangram::Expr;

package Tangram;

use vars qw( $TRACE );
$TRACE = \*STDERR if exists $ENV{TANGRAM_TRACE} && $ENV{TANGRAM_TRACE};

package Tangram::Relational;

sub connect
  {
	my $self = shift;
	return Tangram::Storage->connect( @_ );
  }

sub deploy
  {
	shift; # class
	shift->deploy( @_ );
  }

sub retreat
  {
	shift; # class
	shift->retreat( @_ );
  }

1;
