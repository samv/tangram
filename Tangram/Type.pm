# (c) Sound Object Logic 2000-2001

use strict;

package Tangram::Type;

use Carp;

my %instances;

sub instance
{
	my $pkg = shift;
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

sub remote_expr
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

sub expr
{
	return Tangram::Expr->new( @_ );
}

sub get_exporters
  {
	my ($self, $fields, $context) = @_;
	return map { $fields->{$_}->get_exporter($context) } keys %$fields;
  }

sub get_importer
  {
	my $type = ref shift();
	die "$type does not implement new get_importer method";
  }

sub get_exporter
  {
	my $type = ref shift();
	die "$type does not implement new get_exporter method";
  }

sub get_export_cols
  {
	()
  }

1;
