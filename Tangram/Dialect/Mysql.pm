use strict;

package Tangram::Dialect::Mysql;

use Tangram::Dialect;
use base qw( Tangram::Dialect );

sub rate_connect_string
  {
    my ($self, $cs) = @_;
    return +($cs =~ m/^dbi:mysql:/i);
  }

sub make_id
  {
    shift;
    my ($storage, $class_id) = @_;

	my $table = $storage->{schema}{class_table};

	$storage->sql_do("UPDATE $table SET lastObjectId = LAST_INSERT_ID(lastObjectId + 1) WHERE classId = $class_id");

    return sprintf "%d%0$storage->{cid_size}d",
	  $storage->sql_selectall_arrayref("SELECT LAST_INSERT_ID()")->[0][0],
		$class_id;
  }

sub tx_start
  {
    shift;
    my $storage = shift;
    $storage->sql_do(q/SELECT GET_LOCK("tx", 10)/)
      if @{ $storage->{tx} };
    $storage->std_tx_start(@_);
  }

sub tx_commit
  {
    shift;
    my $storage = shift;
    $storage->std_tx_commit(@_);
    $storage->sql_do(q/SELECT RELEASE_LOCK("tx")/)
      if @{ $storage->{tx} };
  }

sub tx_rollback
  {
    shift;
    my $storage = shift;
    $storage->sql_do(q/SELECT RELEASE_LOCK("tx")/);
    $storage->std_tx_rollback(@_);
  }

my %improved_date =
  (
   'Tangram::RawDateTime' => 'Tangram::Dialect::Mysql::DateExpr',
   'Tangram::RawDate' => 'Tangram::Dialect::Mysql::DateExpr',
  );

sub expr
  {
    my $self = shift;
    my $type = shift;
    my ($expr, @remotes) = @_;
	
	return Tangram::Dialect::Mysql::IntegerExpr->new($type, $expr, @remotes)
	  if $type->isa('Tangram::Integer');

    my $improved_date = $improved_date{ref($type)};
    return $improved_date->new($type, $expr, @remotes)
	  if $improved_date;

	return $type->expr(@_);
  }

Tangram::Dialect::Mysql->register();

package Tangram::Dialect::Mysql::IntegerExpr;
use base qw( Tangram::Expr );

sub bitwise_and
{
	my ($self, $val) = @_;
	return Tangram::Integer->expr("$self->{expr} & $val", $self->objects);
}

sub bitwise_nand
{
	my ($self, $val) = @_;
	return Tangram::Integer->expr("~$self->{expr} & $val",
							 $self->objects);
}

sub bitwise_or
{
	my ($self, $val) = @_;
	return Tangram::Integer->expr("$self->{expr} | $val", $self->objects);
}

sub bitwise_nor
{
	my ($self, $val) = @_;
	return Tangram::Integer->expr("~$self->{expr} | $val", $self->objects);
}

package Tangram::Dialect::Mysql::DateExpr;
use base qw( Tangram::Expr );

my %autofun = (
			   dayofweek => 'Integer',
			   weekday => 'Integer',
			   dayofmonth => 'Integer',
			   dayofyear => 'Integer',
			   month => 'Integer',
			   dayname => 'String',
			   monthname => 'String',
			   quarter => 'Integer',
			   week => 'Integer',
			   year => 'Integer',
			   yearweek => 'Integer',
			   to_days => 'Integer',
			   unix_timestamp => 'Integer',
			  );

use vars qw( $AUTOLOAD );
use Carp;

sub AUTOLOAD
  {
   my ($self) = @_;

   my ($fun) = $AUTOLOAD =~ /\:\:(\w+)$/;

   croak "Unknown method '$fun'"
	 unless exists $autofun{$fun};

	eval <<SUBDEF;
sub $fun
{
	my (\$self, \$part) = \@_;
	my \$expr = \$self->expr();

	return Tangram\:\:$autofun{$fun}->expr("\U$fun\E(\$expr)", \$self->objects);
}
SUBDEF

  goto &$fun;
}

1;





