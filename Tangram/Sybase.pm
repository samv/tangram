# (c) Sound Object Logic 2000-2001

# The old man page for Tangram::Dialect, reproduced here, is incorrect
# but documents this code well so is included here.

#=head1 CLASS METHODS
#
#=head2 new()
#
#Returns a new Dialect object.
#
#=head1 INSTANCE METHODS
#
#=head2 expr($type, $expr, @remotes)
#
#Returns a new Expr object. The object is obtained by calling method
#expr() on $type. See L<Tangram::Expr> for a description of the
#arguments.
#
#=head1 EXAMPLE
#
#The following code adds support for Sybase's C<datepart>
#extension. See below how to actually I<use> the extension.
#
# ...
#
# To take advantage of the new Dialect, we must pass it to the connect()
# method:
# 
# 
#   my $storage = Tangram::Storage->connect($schema,
#      $data_source, $user, $passwd,
#      { dialect => 'Tangram::Dialect::Sybase' } );
#
#
#Now we can filter on the various parts of a Date:
#
#
#   my $remote = $storage->remote('NaturalPerson');
#
#   my ($person) = $storage->select($remote,
#      $remote->{birth}->datepart('year') == 1963);
#
#
#=head1 SEE ALSO
#
#L<Tangram::Type>, L<Tangram::Expr>, L<Tangram::Storage>.
use strict;

package Tangram::Sybase;
use vars qw(@ISA);
 @ISA = qw( Tangram::Relational );

sub connect {
    my ($pkg, $schema, $cs, $user, $pw, $opts) = @_;
    ${$opts||={}}{driver} = $pkg->new();
    my $storage = Tangram::Sybase::Storage->connect
	( $schema, $cs, $user, $pw, $opts );
}

use Tangram::Storage;

package Tangram::Sybase::Storage;

use vars qw(@ISA);
 @ISA = qw( Tangram::Storage );

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
use vars qw(@ISA);
 @ISA = qw( Tangram::Expr );

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
