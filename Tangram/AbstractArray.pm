# (c) Sound Object Logic 2000-2001

use strict;

use Tangram::Coll;

package Tangram::AbstractArray;

use base qw( Tangram::Coll );

use Carp;

sub demand
{
	my ($self, $def, $storage, $obj, $member, $class) = @_;

	print $Tangram::TRACE "loading $member\n" if $Tangram::TRACE;
   
	my @coll;

	if (my $prefetch = $storage->{PREFETCH}{$class}{$member}{$storage->id($obj)})
	{
		@coll = @$prefetch;
	}
	else
	{
		my $cursor = $self->cursor($def, $storage, $obj, $member);

		for (my $item = $cursor->select; $item; $item = $cursor->next)
		{
			my $slot = shift @{ $cursor->{-residue} };
			$coll[$slot] = $item;
		}
	}

	$self->set_load_state($storage, $obj, $member, [ map { $_ && $storage->id($_) } @coll ]);

	return \@coll;
}

sub get_export_cols
{
  return (); # arrays are not stored on object's table
}

sub save_content
  {
	my ($obj, $field, $context) = @_;

	# has collection been loaded? if not, then it hasn't been modified
	return if tied $obj->{$field};

	my $storage = $context->{storage};

	foreach my $item (@{ $obj->{$field} }) {
	  $storage->insert($item)
		unless $storage->id($item);
	}
  }

sub deep_save_content
  {
	my ($obj, $field, $context) = @_;

	# has collection been loaded? if not, then it hasn't been modified
	return if tied $obj->{$field};

	my $storage = $context->{storage};

	foreach my $item (@{$obj->{$field}}) {
	  $storage->_save($item, $context->{SAVING});
	}
  }

sub check_content
  {
	my ($obj, $field, $coll, $class) = @_;

	foreach my $item ($obj->{$field}) {
	  Tangram::Coll::bad_type($obj, $field, $class, $item)
		unless $item->isa($class);
	}
  }

sub get_exporter
  {
	my ($self, $context) = @_;
	my $save_content = $self->{deep_update} ? \&deep_save_content : \&save_content;
	my $field = $self->{name};

	return sub {
	  my ($obj, $context) = @_;
	  $save_content->($obj, $self->{name}, $context);
	  $context->{storage}->defer(sub { $self->defered_save(shift, $obj, $field, $self) } );
	  ();
	}
  }

sub defered_save
  {
	use integer;
	
	my ($self, $storage, $obj, $field, $def) = @_;
	
	return if tied $obj->{$field}; # collection has not been loaded, thus not modified
	
	my $coll_id = $storage->id($obj);
	
	my ($ne, $modify, $add, $remove) =
	  $self->get_save_closures($storage, $obj, $def, $storage->id($obj));
	
	my $new_state = $obj->{$field} || [];
	my $new_size = @$new_state;
	
	my $old_state = $self->get_load_state($storage, $obj, $field) || [];
	my $old_size = @$old_state;
	
	my ($common, $changed) = Tangram::Coll::array_diff($new_state, $old_state, $ne);
	
	for my $slot (@$changed)
	  {
		$modify->($slot, $new_state->[$slot], $old_state->[$slot]);
	  }
	
	for my $slot ($old_size .. ($new_size-1))
	  {
		$add->($slot, $new_state->[$slot]);
	  }
	
	if ($old_size > $new_size)
	  {
		$remove->($new_size, $old_size);
	  }
	
	$self->set_load_state($storage, $obj, $field, [ @$new_state ] );	
	
	$storage->tx_on_rollback( sub { $self->set_load_state($storage, $obj, $field, $old_state) } );
  }

1;
