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

use vars qw( $TRACE $DEBUG_LEVEL );
$TRACE = (\*STDOUT, \*STDERR)[$ENV{TANGRAM_TRACE} - 1] || \*STDERR
  if exists $ENV{TANGRAM_TRACE} && $ENV{TANGRAM_TRACE};


$DEBUG_LEVEL = 0;

package Tangram::Core;

use vars qw(@ISA @EXPORT @EXPORT_OK);

BEGIN {
    @ISA = qw(Exporter);
    @EXPORT = qw();
    @EXPORT_OK = qw(pretty);
}
sub pretty {
    my $thingy = shift;

    if (defined($thingy)) {
	return "`$thingy'";
    } else {
	return undef;
    }
}

1;
