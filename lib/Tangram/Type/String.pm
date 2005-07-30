
package Tangram::Type::String;

use Tangram::Type::Scalar;
use strict;

use vars qw(@ISA);
 @ISA = qw( Tangram::Type::Scalar );

$Tangram::Schema::TYPES{string} = __PACKAGE__->new;

sub literal
  {
    my ($self, $lit, $storage) = @_;
    return $storage->{db}->quote($lit);
}

1;



