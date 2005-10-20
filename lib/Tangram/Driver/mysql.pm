

use strict;
use Tangram::Core;

package Tangram::Driver::mysql;

use vars qw(@ISA);
 @ISA = qw( Tangram::Relational );

sub connect
  {
      my ($pkg, $schema, $cs, $user, $pw, $opts) = @_;
      ${$opts||={}}{driver} = $pkg->new();
      my $storage = Tangram::Driver::mysql::Storage->connect
	  ( $schema, $cs, $user, $pw, $opts );
  }

# FIXME - this should be implemented in the same way as the
# IntegerExpr stuff, below.
sub dbms_date {
    my $self = shift;

    my $date = $self->SUPER::dbms_date(shift);

    # convert standard ISO-8601 to a format that MySQL natively
    # understands, dumbass that it is.
    $date =~ s{^(\d{4})(\d{2})(\d{2})(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)$}
	{$1-$2-$3 $4:$5:$6};

    return $date;
}

sub sequence_sql {
    my $self = shift;

    my $sequence_name = shift;
    # from the MySQL manual
    # http://dev.mysql.com/doc/mysql/en/Information_functions.html
    return("UPDATE seq_$sequence_name SET id=LAST_INSERT_ID(id+1);\n"
	   ."SELECT LAST_INSERT_ID();");
}

sub mk_sequence_sql {
    my $self = shift;
    my $sequence_name = shift;

    return("CREATE TABLE seq_$sequence_name (id INT NOT NULL);\n"
	   ."INSERT INTO seq_$sequence_name VALUES (0);");
}

sub drop_sequence_sql {
    my $self = shift if ref $_[0] and UNIVERSAL::isa($_[0], __PACKAGE__);
    my $sequence_name = shift;
    return "DROP TABLE seq_$sequence_name";
}

sub limit_sql {
    my $self = shift;
    return (limit => shift);
}

package Tangram::Driver::mysql::Storage;

use Tangram::Storage;
use vars qw(@ISA);
 @ISA = qw( Tangram::Storage );

sub make_id
  {
    my ($storage, $class_id) = @_;

	if ($storage->{layout1}) {
	  my $table = $storage->{schema}{class_table};
	  $storage->sql_do("UPDATE $table SET lastObjectId = LAST_INSERT_ID(lastObjectId + 1) WHERE classId = $class_id");
	} else {
	  my $table = $storage->{schema}{control};
	  $storage->sql_do("UPDATE $table SET mark = LAST_INSERT_ID(mark + 1)");
	}

    return sprintf "%d%0$storage->{cid_size}d", $storage->sql_selectall_arrayref("SELECT LAST_INSERT_ID()")->[0][0], $class_id;
  }

sub tx_start
  {
    my $storage = shift;
    unless (@{ $storage->{tx} }) {
	if ( $storage->{no_tx} ) {
	    $storage->sql_do (q{SELECT GET_LOCK("tx", 10)} ); #})  #cperl-mode--
	}
    }
    $storage->SUPER::tx_start(@_);
  }

sub tx_commit
  {
    my $storage = shift;
    $storage->SUPER::tx_commit(@_);
    unless (@{ $storage->{tx} }) {
	if ( $storage->{no_tx} ) {
	    $storage->sql_do(q/SELECT RELEASE_LOCK("tx")/)
	}
    }
  }

sub tx_rollback
  {
    my $storage = shift;
    if ( $storage->{no_tx} ) {
	$storage->sql_do(q/SELECT RELEASE_LOCK("tx")/);
    }
    $storage->SUPER::tx_rollback(@_);
  }

my %improved_date =
  (
   'Tangram::Type::TimeAndDate' => 'Tangram::mysql::DateExpr',
   'Tangram::Type::Date' => 'Tangram::mysql::DateExpr',
  );

sub expr
  {
    my $self = shift;
    my $type = shift;
    my ($expr, @remotes) = @_;

	return Tangram::Driver::mysql::Expr::Integer->new($type, $expr, @remotes)
	  if ref($type) eq 'Tangram::Type::Integer';

    my $improved_date = $improved_date{ref($type)};
    return $improved_date->new($type, $expr, @remotes)
	  if $improved_date;

	return $type->expr(@_);
  }

package Tangram::Driver::mysql::Expr::Integer;
use vars qw(@ISA);
 @ISA = qw( Tangram::Expr );

sub bitwise_and
{
	my ($self, $val) = @_;
	return Tangram::Type::Integer->expr("$self->{expr} & $val", $self->objects);
}

sub bitwise_nand
{
	my ($self, $val) = @_;
	return Tangram::Type::Integer->expr("~$self->{expr} & $val",
							 $self->objects);
}

sub bitwise_or
{
	my ($self, $val) = @_;
	return Tangram::Type::Integer->expr("$self->{expr} | $val", $self->objects);
}

sub bitwise_nor
{
	my ($self, $val) = @_;
	return Tangram::Type::Integer->expr("~$self->{expr} | $val", $self->objects);
}

package Tangram::Driver::mysql::Expr::Date;
use vars qw(@ISA);
 @ISA = qw( Tangram::Expr );

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





