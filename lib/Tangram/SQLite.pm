
use strict;
use Tangram::Core;

package Tangram::SQLite;

use vars qw(@ISA);
 @ISA = qw( Tangram::Relational );

sub connect
  {
      my ($pkg, $schema, $cs, $user, $pw, $opts) = @_;
      ${$opts||={}}{driver} = $pkg->new();
      my $storage = Tangram::SQLite::Storage->connect
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

# conversions necessary to binary-safe data


# function to return a DBMS date from an ISO-8601 date in the form:
sub to_date {
    my $self = shift;

    my $date = shift;

    $date =~ s{^(\d{4})(\d{2})(\d{2})(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)$}
	{$1-$2-$3T$4:$5:$6};

    return $date;
}

sub from_date {
    my $self = shift;

    my $date = $self->SUPER::from_date(shift);

    $date =~ s{^(\d{4})(\d{2})(\d{2})(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)$}
	{$1-$2-$3T$4:$5:$6};

    return $date;
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

package Tangram::SQLite::Storage;

use Tangram::Storage;
use vars qw(@ISA);
 @ISA = qw( Tangram::Storage );

sub connect
{
    my $class = shift;

    my $self = $class->SUPER::connect(@_);

    $self->{db}->{RaiseError} = 1;
    #$self->{db}->{sqlite_handle_binary_nulls} = 1;
    return $self;
}


sub has_tx()         { 1 }
sub has_subselects() { 1 }
sub from_dual()      { " FROM DUAL" }

1;
