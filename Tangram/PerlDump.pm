use strict;

use Tangram::Scalar;

package Tangram::PerlDump;

use base qw( Tangram::String );
use Data::Dumper;

$Tangram::Schema::TYPES{perl_dump} = Tangram::PerlDump->new;

my $DumpMeth = (defined &Data::Dumper::Dumpxs) ? 'Dumpxs' : 'Dump';

sub reschema {
  my ($self, $members, $class) = @_;

  if (ref($members) eq 'ARRAY') {
    # short form
    # transform into hash: { fieldname => { col => fieldname }, ... }
    $_[1] = map { $_ => { col => $_ } } @$members;
    return @$members;
    die 'coverok';
  }
    
  for my $field (keys %$members) {
    my $def = $members->{$field};
    my $refdef = ref($def);
    
    unless ($refdef) {
      # not a reference: field => field
      $members->{$field} = { col => $def || $field };
      next;
    }

    die ref($self), ": $class\:\:$field: unexpected $refdef"
      unless $refdef eq 'HASH';
	
    $def->{col} ||= $field;
    $def->{sql} ||= 'VARCHAR(255)';
    $def->{indent} ||= 0;
    $def->{terse} ||= 1;
    $def->{purity} ||= 0;
    $def->{dumper} = sub {
      $Data::Dumper::Indent = $def->{indent};
      $Data::Dumper::Terse  = $def->{terse};
      $Data::Dumper::Purity = $def->{purity};
      $Data::Dumper::Varname = '_t::v';
      Data::Dumper->$DumpMeth([@_], []);
    };
  }

  return keys %$members;
}

sub read
{
    my ($self, $row, $obj, $members) = @_;
    @$obj{keys %$members} =
      map
	{
	  my $v = eval($_);
	  die "Error in undumping perl object \'$v\': $@" if ($@);
	  $_ = $v;
	}
        splice @$row, 0, keys %$members;
}


sub save {
  my ($self, $cols, $vals, $obj, $members, $storage) = @_;
  
  my $dbh = $storage->{db};
  
  foreach my $member (keys %$members) {
    my $memdef = $members->{$member};
    
    next if $memdef->{automatic};
    
    push @$cols, $memdef->{col};
    push @$vals, $dbh->quote(&{$memdef->{dumper}}($obj->{$member}));
  }
}

1;
