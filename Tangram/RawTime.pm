use strict;

use Tangram::Scalar;

package Tangram::RawTime;

use base qw( Tangram::String );

$Tangram::Schema::TYPES{rawtime} = Tangram::RawTime->new;

sub Tangram::RawTime::coldefs
{
    my ($self, $cols, $members) = @_;
    $self->_coldefs($cols, $members, 'TIME NULL');
}

1;
