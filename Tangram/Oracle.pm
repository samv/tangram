# (c) Sound Object Logic 2000-2001

use strict;
use Tangram::Core;

package Tangram::Oracle;

use vars qw(@ISA);
 @ISA = qw( Tangram::Relational );

sub connect
  {
      my ($pkg, $schema, $cs, $user, $pw, $opts) = @_;
      ${$opts||={}}{driver} = $pkg->new();
      my $storage = Tangram::Oracle::Storage->connect
	  ( $schema, $cs, $user, $pw, $opts );
  }

sub blob {
    return "VARCHAR(2048)";
}

sub date {
    return "DATE";
}

sub bool {
    return "INT(1)";
}

package Tangram::Oracle::Storage;

use Tangram::Storage;
use vars qw(@ISA);
 @ISA = qw( Tangram::Storage );

sub connect
{
    my $class = shift;

    my $self = $class->SUPER::connect(@_);

    # Oracle doesn't really have a default date format (locale
    # dependant), so adjust it to use ISO-8601.
    $self->{db}->do
	("ALTER SESSION SET NLS_DATE_FORMAT='YYYYMMDDHH24:MI:SS'");

    return $self;
}


1;
