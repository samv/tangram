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

use vars qw( $TRACE $DEBUG_LEVEL @EXPORT_OK );
$TRACE = (\*STDOUT, \*STDERR)[$ENV{TANGRAM_TRACE} - 1] || \*STDERR
  if exists $ENV{TANGRAM_TRACE} && $ENV{TANGRAM_TRACE};

use base qw(Exporter);

BEGIN {
    @EXPORT_OK = qw(pretty);
}

$DEBUG_LEVEL = 0;

sub pretty {
    my $thingy = shift;

    if (defined($thingy)) {
	return "`$thingy'";
    } else {
	return undef;
    }
}

1;
