use strict;

############################################
# derive a DateExpr class from existing Expr

package Tangram::Dialect::Sybase::DateExpr;
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

###########################
# subclass Tangram::Dialect

package Tangram::Dialect::Sybase;
use Tangram::Dialect;
use base qw( Tangram::Dialect );

############################################
# a hash that maps date-related Types to the
# DateExpr - the improved Expr class

my %improved =
	(
    'Tangram::RawDateTime' => 'Tangram::Dialect::Sybase::DateExpr',
	'Tangram::RawDate' => 'Tangram::Dialect::Sybase::DateExpr',
	);

######################################################
# Tangram calls this method to obtain new Expr objects

sub expr
{
	my ($self, $type, $expr, @remotes) = @_;

	###################################################
	# is $type related to dates? if not, return default

	my $improved = $improved{ref($type)} or return $type->expr(@_);
	
	####################################
	# $type is a Date; return a DateExpr

	return $improved->new($type, $expr, @remotes);
}

1;





