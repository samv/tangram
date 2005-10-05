

package Tangram;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

require Exporter;

@ISA = qw(Exporter AutoLoader);
# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.
@EXPORT = qw(

);

{ local($^W) = 0;
$VERSION = '2.10';
my $force_numeric = $VERSION + 0;
}

# Preloaded methods go here.

use Tangram::Core;

use Tangram::Type::Set::FromMany;
use Tangram::Type::Set::FromOne;

use Tangram::Type::Array::FromMany;
use Tangram::Type::Array::FromOne;

use Tangram::Type::Hash::FromMany;
use Tangram::Type::Hash::FromOne;

sub connect
  {
	shift;
	Tangram::Storage->connect( @_ );
  }

1;

__END__
