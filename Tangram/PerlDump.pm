# (c) Sound Object Logic 2000-2001

# Copyright 1999-2001 Gabor Herr. All rights reserved.
# This program is free software; you can redistribute it and/or modify it
# under the same terms as Perl itself

# Modified 29dec2000 by Jean-Louis Leroy
# replaced save() by get_exporter()
# fixed reschema(): $def->{dumper} was not set when using abbreviated forms

use strict;

use Tangram::Scalar;

package Tangram::PerlDump;

use vars qw(@ISA);
 @ISA = qw( Tangram::String );
use Data::Dumper;

$Tangram::Schema::TYPES{perl_dump} = Tangram::PerlDump->new;

my $DumpMeth = (defined &Data::Dumper::Dumpxs) ? 'Dumpxs' : 'Dump';

sub reschema {
  my ($self, $members, $class, $schema) = @_;

  if (ref($members) eq 'ARRAY') {
    # short form
    # transform into hash: { fieldname => { col => fieldname }, ... }
    $_[1] = map { $_ => { col => $schema->{normalize}->($_, 'colname') } } @$members;
  }
    
  for my $field (keys %$members) {
    my $def = $members->{$field};
    my $refdef = ref($def);
    
    unless ($refdef) {
      # not a reference: field => field
      $def = $members->{$field} = { col => $schema->{normalize}->(($def || $field), 'colname') };
	  $refdef = ref($def);
    }

    die ref($self), ": $class\:\:$field: unexpected $refdef"
      unless $refdef eq 'HASH';
	
    $def->{col} ||= $schema->{normalize}->($field, 'colname');
    $def->{sql} ||= 'VARCHAR(255)';
    $def->{indent} ||= 0;
    $def->{terse} ||= 1;
    $def->{purity} ||= 0;
    $def->{dumper} ||= sub {
      $Data::Dumper::Indent = $def->{indent};
      $Data::Dumper::Terse  = $def->{terse};
      $Data::Dumper::Purity = $def->{purity};
      $Data::Dumper::Varname = '_t::v';
      Data::Dumper->$DumpMeth([@_], []);
    };
  }

  return keys %$members;
}

sub get_importer
{
	my ($self, $context) = @_;
	return "\$obj->{$self->{name}} = eval shift \@\$row";
  }

sub get_exporter
  {
	my ($self, $context) = @_;
	my $field = $self->{name};

	return sub {
	  my ($obj, $context) = @_;
	  $self->{dumper}->($obj->{$field});
	};
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
