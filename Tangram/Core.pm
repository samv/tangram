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

	my $dialect = 'Tangram::' . (split ':', $_[1])[1]; # deduce dialect from DBI driver

	eval "use $dialect";

	return Tangram::Storage->connect( @_ )
	  if $@	|| $dialect->can('connect') == \&connect;

	print $Tangram::TRACE "Using $dialect\n"
	  if $Tangram::TRACE;

	return $dialect->connect( @_ );
  }

1;
