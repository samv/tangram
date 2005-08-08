

use strict;

use Tangram::Scalar;

package Tangram::Type/TimeAndDate;

use vars qw(@ISA);
 @ISA = qw( Tangram::String );

$Tangram::Schema::TYPES{rawdatetime} = Tangram::Type/TimeAndDate->new;

sub coldefs
{
    my ($self, $cols, $members, $schema) = @_;
    $self->_coldefs($cols, $members, "DATETIME $schema->{sql}{default_null}");
}

1;
