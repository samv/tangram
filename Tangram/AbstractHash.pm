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

    $storage->{scratch}{ref($self)}{$storage->id($obj)}{$member} = {
																	map { $_ => ($coll{$_} && $storage->id( $coll{$_} ) ) } keys %coll };

    return \%coll;
}

1;
