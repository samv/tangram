# (c) Sound Object Logic 2000-2001

package Tangram::DMDateTime;

use strict;
use Tangram::RawDateTime;
use base qw( Tangram::RawDateTime );

$Tangram::Schema::TYPES{dmdatetime} = Tangram::DMDateTime->new;

#
# Convert SQL DATETIME format to Date::Manip internal format (0):
#
#   2000-07-06 13:55:23  --> 20000706135523
#
# save is not needed, because most DBMs can directly interpret Date::Manip
# format. 
#
sub get_importer
{
  my ($self) = @_;
  my $name = $self->{name};

  return sub {
	my ($obj, $row, $context) = @_;
	my $val = shift @$row;
	$val =~ s/[-: ]//g;
	$obj{$name} = $val;
  }
}

1;
