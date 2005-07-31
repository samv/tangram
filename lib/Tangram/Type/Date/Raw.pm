

use strict;

package Tangram::Type::Date::Raw;

use Tangram::Type::String;
use vars qw(@ISA);
 @ISA = qw( Tangram::Type::String );

$Tangram::Schema::TYPES{rawdate} = __PACKAGE__->new;

sub Tangram::RawDate::coldefs
{
    my ($self, $cols, $members, $schema) = @_;
    $self->_coldefs($cols, $members, "DATE $schema->{sql}{default_null}");
}

1;
