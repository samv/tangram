use strict;

use Tangram::Scalar;

package Tangram::RawDateTime;

use base qw( Tangram::String );

$Tangram::Schema::TYPES{rawdatetime} = Tangram::RawDateTime->new;

sub Tangram::RawDateTime::coldefs
{
    my ($self, $cols, $members) = @_;
    $self->_coldefs($cols, $members, 'DATETIME NULL');
}

1;
