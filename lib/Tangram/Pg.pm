
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
    return "BYTEA";
}

sub date {
    return "DATE";
}

sub bool {
    return "BOOL";
}

use MIME::Base64;

sub to_blob {
    my $self = shift;
    my $value = shift;
    encode_base64($value);
}

sub from_blob {
    my $self = shift;
    my $value = shift;
    decode_base64($value);
}

sub sequence_sql {
    my $self = shift;
    my $sequence_name = shift;
    return "SELECT nextval('$sequence_name')";
}

sub limit_sql {
    my $self = shift;
    return (limit => shift);
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
