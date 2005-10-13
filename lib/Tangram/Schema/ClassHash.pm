package Tangram::Schema::ClassHash;
use strict;

use strict;
use Carp;

sub class
{
   my ($self, $class) = @_;
   $self->{$class} or croak "unknown class '$class'";
}

1;
