
use strict;
use Tangram::Core;

package Tangram::Pg;

use vars qw(@ISA);
 @ISA = qw( Tangram::Relational );

sub connect
  {
      my ($pkg, $schema, $cs, $user, $pw, $opts) = @_;
      ${$opts||={}}{driver} = $pkg->new();
      my $storage = Tangram::Pg::Storage->connect
	  ( $schema, $cs, $user, $pw, $opts );
  }

sub blob {
    return "BLOB";
}

sub date {
    return "DATE";
}

sub bool {
    return "BOOL";
}

# FIXME - this should be implemented in the same way as the
# IntegerExpr stuff, below.
sub dbms_date {
    my $self = shift;

    my $date = $self->SUPER::dbms_date(shift);

    # convert standard ISO-8601 to a format that MySQL natively
    # understands, dumbass that it is.
    $date =~ s{^(\d{4})(\d{2})(\d{2})(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)$}
	{$1-$2-$3T$4:$5:$6};

    return $date;
}

package Tangram::Pg::Storage;

use Tangram::Storage;
use vars qw(@ISA);
 @ISA = qw( Tangram::Storage );

sub connect
{
    my $class = shift;

    my $self = $class->SUPER::connect(@_);

    $self->{db}->{RaiseError} = 1;
    return $self;
}


sub has_tx()         { 1 }
sub has_subselects() { 1 }
sub from_dual()      { " FROM DUAL" }

1;
