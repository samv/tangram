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

    $storage->{scratch}{ref($self)}{$storage->id($obj)}{$member} = {
																	map { $_ => ($coll{$_} && $storage->id( $coll{$_} ) ) } keys %coll };

    return \%coll;
}

sub save
{
    my ($self, $cols, $vals, $obj, $members, $storage, $table, $id) = @_;

    foreach my $coll (keys %$members)
    {
		next if tied $obj->{$coll};
		next unless defined $obj->{$coll};

		my $class = $members->{$coll}{class};

		foreach my $item (values %{ $obj->{$coll} } )
		{
			Tangram::Coll::bad_type($obj, $coll, $class, $item)
					if $^W && !$item->isa($class);
			if ($members->{$coll}->{deep_update}) {
				$storage->_save($item);
			} else {
				$storage->insert($item)	unless $storage->id($item);
		        }
		}
    }

    $storage->defer(sub { $self->defered_save(shift, $obj, $members, $id) } );

    return ();
}

1;
