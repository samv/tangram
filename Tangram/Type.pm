use strict;

package Tangram::Type;

use Carp;

my %instances;

sub instance
{
	my $pkg = shift;
	Carp::confess "no arguments '@_' allowed" if @_;
	return $instances{$pkg} ||= bless { }, $pkg;
}

*new = \&instance;

sub reschema
{
}

sub members
{
   my ($self, $members) = @_;
   keys %$members;
}

sub query_expr
{
}

sub erase
{
}

sub read_data
{
	my ($self, $row) = @_;
	shift @$row;
}

sub read
{
   my ($self, $row, $obj, $members) = @_;
	
	foreach my $key (keys %$members)
	{
		$obj->{$key} = $self->read_data($row)
	}
}

sub prefetch
{
}

1;