# (c) Sound Object Logic 2000-2001

use strict;

package Tangram::AbstractHash;

use Tangram::Coll;
use base qw( Tangram::Coll );

use Carp;

sub content
{
    shift;
    @{shift()};
}

sub demand
{
    my ($self, $def, $storage, $obj, $member, $class) = @_;

    print $Tangram::TRACE "loading $member\n" if $Tangram::TRACE;
    
    my %coll;

    if (my $prefetch = $storage->{PREFETCH}{$class}{$member}{$storage->id($obj)})
    {
		%coll = %$prefetch;
    }
    else
    {
		my $cursor = $self->cursor($def, $storage, $obj, $member);

		for (my $item = $cursor->select; $item; $item = $cursor->next)
		{
			my $slot = shift @{ $cursor->{-residue} };
			$coll{$slot} = $item;
		}
    }

	$self->set_load_state($storage, $obj, $member, map { $_ => ($coll{$_} && $storage->id( $coll{$_} ) ) } keys %coll );

    return \%coll;
}

sub save_content
  {
	my ($obj, $field, $context) = @_;

	# has collection been loaded? if not, then it hasn't been modified
	return if tied $obj->{$field};
	return unless exists $obj->{$field} && defined $obj->{$field};
	
	my $storage = $context->{storage};

	foreach my $item (values %{ $obj->{$field} }) {
	  $storage->insert($item)
		unless $storage->id($item);
	}
  }

sub get_exporter
  {
	my ($self, $context) = @_;
	my $field = $self->{name};

	return sub {
	  my ($obj, $context) = @_;

	  return if tied $obj->{$field};
	  return unless exists $obj->{$field} && defined $obj->{$field};
	
	  my $storage = $context->{storage};

	  foreach my $item (values %{ $obj->{$field} }) {
		$storage->insert($item)
		  unless $storage->id($item);
	  }

	  $context->{storage}->defer(sub { $self->defered_save($obj, $field, $storage) } );
	  ();
	}
  }


1;
