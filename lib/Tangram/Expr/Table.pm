package Tangram::Expr::Table;

use strict;

# This is a stub version of Tangram::CursorObject

sub new
{
	my ($pkg, $name, $alias) = @_;
	bless [ $name, $alias ], $pkg;
}

sub from
{
	return "@{shift()}";
}

sub where
{
	()
}

1;
