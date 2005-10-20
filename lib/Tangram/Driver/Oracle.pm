

package Tangram::Driver::Oracle;

use strict;
use Tangram::Core;

use vars qw(@ISA);
 @ISA = qw( Tangram::Relational );

sub connect
  {
      my ($pkg, $schema, $cs, $user, $pw, $opts) = @_;
      ${$opts||={}}{driver} = $pkg->new();
      my $storage = Tangram::Driver::Oracle::Storage->connect
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

sub limit_sql {
    my $self = shift;
    my $spec = shift;
    if ( ref $spec ) {
	die unless ref $spec eq "ARRAY";
	die "Oracle cannot handle two part limits"
	    unless $spec->[0] eq "0";
	$spec = pop @$spec;
    }
    return (postfilter => ["rownum <= $spec"]);
}

package Tangram::Driver::Oracle::Storage;

use Tangram::Storage;
use vars qw(@ISA);
 @ISA = qw( Tangram::Storage );

sub open_connection
{
    my $self = shift;

    my $db = $self->SUPER::open_connection(@_);

    # Oracle doesn't really have a default date format (locale
    # dependant), so adjust it to use ISO-8601.
    $db->do
	("ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD\"T\"HH24:MI:SS'");
    $db->do
	("ALTER SESSION SET CONSTRAINTS = DEFERRED");
    $db->{RaiseError} = 1;
    $db->{LongTruncOk} = 0;
    $db->{LongReadLen} = 1024*1024;
    return $db;
}


sub has_tx()         { 1 }
sub has_subselects() { 1 }
sub from_dual()      { " FROM DUAL" }

1;
