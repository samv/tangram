# (c) Sam Vilain, 2004

package Tangram::CookedDateTime;

use strict;
use Tangram::RawDateTime;
use vars qw(@ISA);
@ISA = qw( Tangram::RawDateTime );

$Tangram::Schema::TYPES{cookeddatetime} = Tangram::CookedDateTime->new;

sub get_importer
{
  my $self = shift;
  my $context = shift;
  my $closure = shift;
  my $name = $self->{name};

  return sub {
	my ($obj, $row, $context) = @_;
	my $val = shift @$row;

	$val = $context->{storage}->from_dbms('date', $val)
	    if defined $val;
	$val = $closure->($val) if defined $val and $closure;

	$obj->{$name} = $val;
  }
}

sub get_exporter
{
    my $self = shift;
    my $context = shift;
    my $closure = shift;
    my $name = $self->{name};

    return sub {
	my ($obj, $context) = @_;
	my $val = $obj->{$name};

	$val = $closure->($val) if defined $val and $closure;
	$val = $context->{storage}->to_dbms('date', $val)
	    if defined $val;

	return $val;
    }
}
1;
