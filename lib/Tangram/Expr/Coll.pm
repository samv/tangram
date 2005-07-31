
package Tangram::Expr::Coll;

use strict;

sub new
{
	my $pkg = shift;
	bless [ @_ ], $pkg;
}

sub exists
{
	my ($self, $expr, $filter) = @_;
	my ($coll) = @$self;

	if ($expr->isa('Tangram::QueryObject'))
	{
		$expr = Tangram::Select->new
			(
			 cols => [ $expr->{id} ],
			 exclude => [ $coll ],
			 filter => $self->includes($expr)->and_perhaps($filter)
			);
	}

	my $expr_str = $expr->{expr};
	$expr_str =~ tr/\n/ /;

	return Tangram::Filter->new( expr => "exists $expr_str", tight => 100,
								 objects => Set::Object->new( $expr->objects() ) );
}

1;
