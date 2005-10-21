

package Tangram;

use strict;

use vars qw( $TRACE $DEBUG_LEVEL );

$TRACE = (\*STDOUT, \*STDERR)[$ENV{TANGRAM_TRACE} - 1] || \*STDERR
  if exists $ENV{TANGRAM_TRACE} && $ENV{TANGRAM_TRACE};

$DEBUG_LEVEL = $ENV{TANGRAM_DEBUG_LEVEL} || 0;

use Exporter;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK @KEYWORDS $KEYWORDS_RE);
@ISA = qw(Exporter AutoLoader);
@EXPORT = qw();
@EXPORT_OK = qw(pretty d);

{ local($^W) = 0;
$VERSION = '2.10_01';
my $force_numeric = $VERSION + 0;
}

# Preloaded methods go here.

BEGIN {
    @KEYWORDS = qw(compat_quiet core);
    $KEYWORDS_RE = qr/^:(?:${\(join "|", map { qr{\Q$_\E} }
                               @KEYWORDS)})/;
}

use Carp;
use Set::Object qw(1.10);
BEGIN { Set::Object->import("set") };

sub import {
    my $package = shift;
    if ( $_[0] =~ m{^\d} ) {
	# they want a specific version, do the test ourselves to avoid
	# a warning
	my $wanted = shift;
	local($^W) = 0;
	carp "Tangram version $wanted required--this is only $VERSION"
	    if $wanted > $VERSION or ( $wanted == $VERSION and
				       $wanted gt $VERSION );
    }

    my @for_exporter = grep !m/$KEYWORDS_RE/, @_;
    my $options = set(grep m/$KEYWORDS_RE/, @_);

    $package->SUPER::import(@for_exporter);

    # don't go requiring extra modules for 
    return if caller =~ m{^Tangram::};

    require Tangram::Core;

    unless ( $options->includes(":core") ) {
	require Tangram::Type::Set::FromMany;
	require Tangram::Type::Set::FromOne;

	require Tangram::Type::Array::FromMany;
	require Tangram::Type::Array::FromOne;

	require Tangram::Type::Hash::FromMany;
	require Tangram::Type::Hash::FromOne;
    }

    unless ( $options->includes(":no_compat") ) {
	require Tangram::Compat;
	if ( $options->includes(":compat_quiet") ) {
	    Tangram::Compat::quiet(scalar caller);
	}
    }
}

sub connect
  {
	shift;
	Tangram::Storage->connect( @_ );
  }

# these modules are "Core"
use Tangram::Type::Scalar;
use Tangram::Type::Ref::FromMany;

use Tangram::Schema;
use Tangram::Cursor;
use Tangram::Storage;
use Tangram::Expr;
use Tangram::Relational;


1;

__END__
