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
$TRACE = \*STDERR if exists $ENV{TANGRAM_TRACE} && $ENV{TANGRAM_TRACE};

1;
