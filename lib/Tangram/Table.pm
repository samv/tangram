
package Tangram::Table;

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
