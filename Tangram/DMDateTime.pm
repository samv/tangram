# (c) Sound Object Logic 2000-2001

package Tangram::DMDateTime;

use strict;
use Tangram::RawDateTime;
our @ISA = qw( Tangram::RawDateTime );
use Date::Manip qw(ParseDate UnixDate);

$Tangram::Schema::TYPES{dmdatetime} = Tangram::DMDateTime->new;

#
# Convert SQL DATETIME format to Date::Manip internal format; assume
# that "ParseDate" will magically do The Right Thing(tm)
#
sub get_importer
{
  my ($self) = @_;
  my $name = $self->{name};

  return sub {
	my ($obj, $row, $context) = @_;
	my $val = shift @$row;
	$val = ParseDate($val) if defined $val;
	$obj->{$name} = $val;
  }
}

#
# Convert Date::Manip internal format (ISO-8601) to format that should
# work with most databases (read: I've only tested with MySQL but the
# value is sensible)
#
sub get_exporter
{
    my $self = shift;

    my $name = $self->{name};

    return sub {
	my ($obj, $row, $context) = @_;
	my $val = $obj->{$name};
	$val = UnixDate($val, "%Y-%m-%d %H:%M:%S") if defined $val;
	$row->{$name} = $val;
    }
}
1;
