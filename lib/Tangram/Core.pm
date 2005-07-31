use strict;

use Set::Object;

use Tangram::Compat;
BEGIN {
}

use Tangram::Scalar;
use Tangram::Ref;

use Tangram::Schema;
use Tangram::Cursor;
use Tangram::Storage;
use Tangram::Expr;
use Tangram::Relational;

package Tangram;
# Why does this package continue here? -- ank

use vars qw( $TRACE $DEBUG_LEVEL );
$TRACE = (\*STDOUT, \*STDERR)[$ENV{TANGRAM_TRACE} - 1] || \*STDERR
  if exists $ENV{TANGRAM_TRACE} && $ENV{TANGRAM_TRACE};

$DEBUG_LEVEL = $ENV{TANGRAM_DEBUG_LEVEL} || 0;

package Tangram::Core;

use Exporter;
use vars qw(@ISA @EXPORT_OK);

BEGIN {
    @ISA = qw(Exporter);
    @EXPORT_OK = qw(pretty);
}

# pretty("bla") -> "`bla'"
# pretty(undef) -> undef
sub pretty {
    my $thingy = shift;
    if (defined($thingy)) {
	return "`$thingy'";
    } else {
	return "undef";
    }
}

1;
