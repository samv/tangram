# (c) Sound Object Logic 2000-2001

use strict;

package Tangram::Sybase;
our @ISA = qw( Tangram::Relational );

sub connect
  {
	shift;
	Tangram::Sybase::Storage->connect( @_ )
  }

use Tangram::Storage;

package Tangram::Sybase::Storage;

our @ISA = qw( Tangram::Storage );

sub prepare
  {
	my ($self, $sql) = @_;
	#print "prepare: $sql\n";
	bless [ $self, $sql ], 'Tangram::Sybase::Statement';
  }

*prepare_update = \*prepare;
*prepare_insert = \*prepare;

sub prepare_select
  {
	my ($self, $sql) = @_;
	return $self->prepare($sql);
  }

sub make_1st_id_in_tx
  {
    my ($self) = @_;
	my $table = $self->{schema}{control};
	$self->sql_do("UPDATE $table SET mark = mark + 1");
	return $self->{db}->selectall_arrayref("SELECT mark from $table")->[0][0];
  }

sub update_id_in_tx
  {
	my ($self, $mark) = @_;
	$self->sql_do("UPDATE $self->{schema}{control} SET mark = $mark");
  }

my %improved =
  (
   'Tangram::RawDateTime' => 'Tangram::Sybase::DateExpr',
   'Tangram::RawDate' => 'Tangram::Sybase::DateExpr',
  );

sub expr
  {
    my $self = shift;
    my $type = shift;
    my ($expr, @remotes) = @_;
    
    # is $type related to dates? if not, return default
    my $improved = $improved{ref($type)} or return $type->expr(@_);
    
    # $type is a Date; return a DateExpr
    return $improved->new($type, $expr, @remotes);
}

package Tangram::Sybase::Statement;

use constant STH => 2;

sub execute
  {
	my $self = shift;
	my ($storage, $sql) = @$self;
	
	my $sth = $self->[STH] = $storage->{db}->prepare($sql);
	$sth->execute(@_);
	
	# $dbh->do($sql, {}, @_);
  }

sub fetchrow_array
  {
	my $self = shift;
	return $self->[STH]->fetchrow_array();
  }

sub finish
  {
	my $self = shift;
	my $sth = pop @$self;
	$sth->finish();
  }

############################################
# derive a DateExpr class from existing Expr

package Tangram::Sybase::DateExpr;
our @ISA = qw( Tangram::Expr );

############################
# add method datepart($part)

sub datepart
{
	my ($self, $part) = @_; # $part is 'year', 'month', etc
	my $expr = $self->expr(); # the SQL string for this Expr

	##################################
	# build a new Expr of Integer type
	# pass this Expr's remote object list to the new Expr

	return Tangram::Integer->expr("DATEPART($part, $expr)", $self->objects);
}

1;
