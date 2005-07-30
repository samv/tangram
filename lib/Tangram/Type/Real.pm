
package Tangram::Type::Real;
use Tangram::Type;
use strict;

use vars qw(@ISA);
 @ISA = qw( Tangram::Type::Number );

$Tangram::Schema::TYPES{real} = __PACKAGE__->new;

sub coldefs
{
    my ($self, $cols, $members, $schema) = @_;
    $self->_coldefs($cols, $members, 'REAL', $schema);
}

1;
