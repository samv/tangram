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
sub read
{
    my ($self, $row, $obj, $members) = @_;
    @$obj{keys %$members} =
      map { s/[-: ]//g; $_; }
        splice @$row, 0, keys %$members;
}

1;
