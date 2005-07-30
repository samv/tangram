
package Tangram::Integer;
use Tangram::Number;
use strict;

use vars qw(@ISA);
 @ISA = qw( Tangram::Number );
$Tangram::Schema::TYPES{int} = Tangram::Integer->new;

1;
