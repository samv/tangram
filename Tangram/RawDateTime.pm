# (c) Sound Object Logic 2000-2001

use strict;

use Tangram::Scalar;

package Tangram::RawDateTime;

use base qw( Tangram::String );

$Tangram::Schema::TYPES{rawdatetime} = Tangram::RawDateTime->new;

sub coldefs
{
    my ($self, $cols, $members, $schema) = @_;
    $self->_coldefs($cols, $members, "DATETIME $schema->{sql}{default_null}");
}

1;
