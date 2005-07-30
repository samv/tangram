
package Tangram::Type::Integer;

use Tangram::Type::Number;
use strict;

use vars qw(@ISA);
 @ISA = qw( Tangram::Type::Number );
$Tangram::Schema::TYPES{int} = __PACKAGE__->new;

1;
