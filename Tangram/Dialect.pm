use strict;

package Tangram::Dialect;

sub new
{
	my $class = shift;
	return bless { @_ }, $class;
}

sub expr
{
	my $self = shift;
	return shift->expr( @_ );
}

1;
