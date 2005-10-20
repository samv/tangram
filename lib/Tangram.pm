

package Tangram;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK @KEYWORDS $KEYWORDS_RE);

require Exporter;

@ISA = qw(Exporter AutoLoader);
# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.
@EXPORT = qw(

);

{ local($^W) = 0;
$VERSION = '2.09_01';
my $force_numeric = $VERSION + 0;
}

# Preloaded methods go here.

BEGIN {
    @KEYWORDS = qw(compat_quiet core);
    $KEYWORDS_RE = qr/^:(?:${\(join "|", map { qr{\Q$_\E} }
                               @KEYWORDS)})/;
}

use Set::Object qw(1.10);
BEGIN { Set::Object->import("set") };

sub import {
    my $package = shift;
    my @for_exporter = grep !m/$KEYWORDS_RE/, @_;
    my $options = set(grep m/$KEYWORDS_RE/, @_);
    $package->SUPER::import(@for_exporter);

    require Tangram::Core;

    unless ( $options->includes(":core") ) {
	require Tangram::Type::Set::FromMany;
	require Tangram::Type::Set::FromOne;

	require Tangram::Type::Array::FromMany;
	require Tangram::Type::Array::FromOne;

	require Tangram::Type::Hash::FromMany;
	require Tangram::Type::Hash::FromOne;
    }

    if ( $options->includes(":compat_quiet") ) {
	Tangram::Compat::quiet(scalar caller);
    }

}

sub connect
  {
	shift;
	Tangram::Storage->connect( @_ );
  }

1;

__END__
