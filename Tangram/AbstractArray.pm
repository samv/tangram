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

	$storage->{scratch}{ref($self)}{$storage->id($obj)}{$member} = [ map { $_ && $storage->id($_) } @coll ];

	return \@coll;
}

sub save
{
	my ($self, $cols, $vals, $obj, $members, $storage, $table, $id) = @_;

	foreach my $coll (keys %$members)
	{
		next if tied $obj->{$coll};

		my $class = $members->{$coll}{class};

		foreach my $item (@{$obj->{$coll}})
		{
			Tangram::Coll::bad_type($obj, $coll, $class, $item) if $^W && !$item->isa($class);
			$storage->insert($item)	unless $storage->id($item);
		}
	}

	$storage->defer(sub { $self->defered_save(shift, $obj, $members, $id) } );

	return ();
}

sub defered_save
{
	use integer;

	my ($self, $storage, $obj, $members, $coll_id) = @_;

	foreach my $member (keys %$members)
	{
		next if tied $obj->{$member}; # collection has not been loaded, thus not modified
		
		my $def = $members->{$member};
		
		my ($ne, $modify, $add, $remove) =
			$self->get_save_closures($storage, $obj, $def, $coll_id);

		my $new_state = $obj->{$member} || [];
		my $new_size = @$new_state;

		my $old_state = $self->get_load_state($storage, $obj, $member) || [];
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

		$self->set_load_state($storage, $obj, $member, [ @$new_state ] );	

		$storage->tx_on_rollback(
            sub { $self->set_load_state($storage, $obj, $member, $old_state) } );
	}
}

1;
