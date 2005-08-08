

use strict;

use Tangram::Scalar;

package Tangram::Type/Date;

use vars qw(@ISA);
 @ISA = qw( Tangram::String );

$Tangram::Schema::TYPES{rawdate} = Tangram::Type/Date->new;

sub Tangram::Type/Date::coldefs
{
    my ($self, $cols, $members, $schema) = @_;
    $self->_coldefs($cols, $members, "DATE $schema->{sql}{default_null}");
}

1;
