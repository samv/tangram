

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
    return "CLOB";
}

sub date {
    return "DATE";
}

sub bool {
    return "INT(1)";
}

# Oracle--
sub from_date {
    $_[1];
    #print STDERR "Converting FROM $_[1]\n";
    #(my $date = $_[1]) =~ s{ }{T};
    #$date;
 }
sub to_date {
    $_[1];
    #print STDERR "Converting TO $_[1]\n";
    #(my $date = $_[1]) =~ s{T}{ };
    #$date;
}

sub from_blob { $_[1] }
sub to_blob { $_[1] }

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
	("ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD\"T\"HH24:MI:SS'");
    $self->{db}->do
	("ALTER SESSION SET CONSTRAINTS = DEFERRED");
    $self->{db}->{RaiseError} = 1;
    $self->{db}->{LongTruncOk} = 1;
    return $self;
}


sub has_tx()         { 1 }
sub has_subselects() { 1 }
sub from_dual()      { " FROM DUAL" }

1;
