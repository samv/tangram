use strict;

package Tangram::Sybase;

sub connect
  {
	shift;
	Tangram::Sybase::Storage->connect( @_ )
  }

use Tangram::Storage;

package Tangram::Sybase::Storage;

use base qw( Tangram::Storage );

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
    my ($self, $class_id) = @_;
    
	my $table = $self->{schema}{class_table};
	$self->sql_do("UPDATE $table SET lastObjectId = lastObjectId + 1 WHERE classId = $class_id");
	return $self->{db}->selectall_arrayref("SELECT lastObjectId from $table WHERE classId =  $class_id")->[0][0];
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

sub execute
  {
	my $self = shift;
	my ($storage, $sql) = @$self;
	my $dbh = $storage->{db};
	$dbh->do($sql, {}, @_);
	#$sql =~ s/\?/$dbh->quote(shift)/ge; 
	#print "$sql\n";
	#$storage->sql_do($sql);

	

  }

sub finish
  {
  }

############################################
# derive a DateExpr class from existing Expr

package Tangram::Sybase::DateExpr;
use base qw( Tangram::Expr );

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
