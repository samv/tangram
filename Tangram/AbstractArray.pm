use strict;

use Tangram::Coll;

package Tangram::AbstractArray;

use base qw( Tangram::Coll );

sub content
{
   shift;
   @{shift()};
}

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

      foreach my $item (@{$obj->{$coll}})
      {
         $storage->insert($item) unless $storage->id($item);
      }
   }

   $storage->defer(sub { $self->defered_save(shift, $obj, $members, $id) } );

   return ();
}

1;
