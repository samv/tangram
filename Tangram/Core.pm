# (c) Sound Object Logic 2000-2001

use strict;

use Set::Object;

use Tangram::Scalar;
use Tangram::Ref;

use Tangram::Schema;
use Tangram::Cursor;
use Tangram::Storage;
use Tangram::Expr;
use Tangram::Relational;

package Tangram;

use vars qw( $TRACE );
$TRACE = (\*STDOUT, \*STDERR)[$ENV{TANGRAM_TRACE} - 1] || \*STDERR
  if exists $ENV{TANGRAM_TRACE} && $ENV{TANGRAM_TRACE};

sub pretty {
    my $thingy = shift;

    if (defined($thingy)) {
	return "`$thingy'";
    } else {
	return undef;
    }
}

1;
