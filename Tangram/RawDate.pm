# (c) Sound Object Logic 2000-2001

use strict;

use Tangram::Scalar;

package Tangram::RawDate;

our @ISA = qw( Tangram::String );

$Tangram::Schema::TYPES{rawdate} = Tangram::RawDate->new;

sub Tangram::RawDate::coldefs
{
    my ($self, $cols, $members, $schema) = @_;
    $self->_coldefs($cols, $members, "DATE $schema->{sql}{default_null}");
}

1;
