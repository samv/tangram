

use strict;

use Tangram::Scalar;

package Tangram::Type/Time;

use vars qw(@ISA);
 @ISA = qw( Tangram::String );

$Tangram::Schema::TYPES{rawtime} = Tangram::Type/Time->new;

sub Tangram::Type/Time::coldefs
{
    my ($self, $cols, $members, $schema) = @_;
    $self->_coldefs($cols, $members, "TIME $schema->{sql}{default_null}");
}

1;
