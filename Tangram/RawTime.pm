use strict;

use Tangram::Scalar;

package Tangram::RawTime;

use base qw( Tangram::String );

$Tangram::Schema::TYPES{rawtime} = Tangram::RawTime->new;

sub Tangram::RawTime::coldefs
{
    my ($self, $cols, $members, $schema) = @_;
    $self->_coldefs($cols, $members, "TIME $schema->{sql}{default_null}");
}

1;
