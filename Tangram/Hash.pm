use strict;

package Tangram::Hash;

use Tangram::AbstractHash;
use base qw( Tangram::AbstractHash );

use Carp;

sub reschema
{
    my ($self, $members) = @_;

    foreach my $member (keys %$members)
    {
		my $def = $members->{$member};

		unless (ref($def))
		{
			$def = { class => $def };
			$members->{$member} = $def;
		}

		$def->{table} ||= $def->{class} . "_$member";
		$def->{coll} ||= 'coll';
		$def->{item} ||= 'item';
		$def->{slot} ||= 'slot';
		$def->{quote} = !exists $def->{key_type} || $def->{key_type} eq 'string' ? "'" : '';
    }
    
    return keys %$members;
}

sub defered_save
{
    my ($self, $storage, $obj, $members, $coll_id) = @_;

    my $old_states = $storage->{scratch}{ref($self)}{$coll_id};

    foreach my $member (keys %$members)
    {
		next if tied($obj->{$member});
		next unless exists $obj->{$member} && defined $obj->{$member};

		my $def = $members->{$member};
		my ($table, $coll_col, $item_col, $slot_col) = @{ $def }{ qw( table coll item slot ) };
		my $Q = $def->{quote};
	
		my $coll = $obj->{$member};

		my $old_state = $old_states->{$member} || {};

		my %removed = %$old_state;
		delete @removed{ keys %$coll };
		my @free = keys %removed;

		my %new_state;

		foreach my $slot (keys %$coll)
		{
			my $item_id = $storage->id($coll->{$slot});

			if (exists $old_state->{$slot})
			{
				# key already exists

				if ($item_id != $old_state->{$slot})
				{
					# val has changed
					$storage->sql_do(
									 "UPDATE $table SET $item_col = $item_id WHERE $coll_col = $coll_id AND $slot_col = $Q$slot$Q" );
				}
			}
			else
			{
				# key does not exist

				if (@free)
				{
					# recycle an existing line
					my $rslot = shift @free;
					$storage->sql_do(
									 "UPDATE $table SET $slot_col = $Q$slot$Q, $item_col = $item_id WHERE $coll_col = $coll_id AND $slot_col = $Q$rslot$Q" );
				}
				else
				{
					# insert a new line
					$storage->sql_do(
									 "INSERT INTO $table ($coll_col, $item_col, $slot_col) VALUES ($coll_id, $item_id, $Q$slot$Q)" );
				}
			}

			$new_state{$slot} = $item_id;

		}						# foreach my $slot (keys %$coll)

		# remove lines in excess

		if (@free)
		{
			@free = map { "$Q$_$Q" } @free if $Q;
			$storage->sql_do( "DELETE FROM $table WHERE $coll_col = $coll_id AND $slot_col IN (@free)" );
		}

		$old_states->{$member} = \%new_state;
		$storage->tx_on_rollback( sub { $old_states->{$member} = $old_state } );
    }
}

sub erase
{
    my ($self, $storage, $obj, $members, $coll_id) = @_;

    foreach my $member (keys %$members)
    {
		my $def = $members->{$member};
	
		my $table = $def->{table} || $def->{class} . "_$member";
		my $coll_col = $def->{coll} || 'coll';
	
		my $sql = "DELETE FROM $table WHERE $coll_col = $coll_id";
		$storage->sql_do($sql);
    }
}

sub cursor						# ?? factorize ??
{
    my ($self, $def, $storage, $obj, $member) = @_;

    my $cursor = Tangram::CollCursor->new($storage, $def->{class}, $storage->{db});

    my $coll_id = $storage->id($obj);
    my $coll_tid = $storage->alloc_table;
    my $table = $def->{table};
    my $item_tid = $cursor->{-stored}->root_table;
    my $coll_col = $def->{coll};
    my $item_col = $def->{item};
    my $slot_col = $def->{slot};
    $cursor->{-coll_tid} = $coll_tid;
    $cursor->{-coll_cols} = ", t$coll_tid.$slot_col";
    $cursor->{-coll_from} = ", $table t$coll_tid";
    $cursor->{-coll_where} = "t$coll_tid.$coll_col = $coll_id AND t$coll_tid.$item_col = t$item_tid.id";
    
    return $cursor;
}

sub query_expr
{
    my ($self, $obj, $members, $tid) = @_;
    map { Tangram::CollExpr->new($obj, $_); } values %$members;
}

sub prefetch
{
    q{
	my ($self, $storage, $def, $coll, $class, $member, $filter) = @_;

	my $ritem = $storage->remote($def->{class});

	# first retrieve the collection-side ids of all objects satisfying $filter
	# empty the corresponding prefetch array

	my $ids = $storage->my_select_data( cols => [ $coll->{id} ], filter => $filter );
	my $prefetch = $storage->{PREFETCH}{$class}{$member} ||= {}; # weakref

	while (my $id = $ids->fetchrow)
	{
	    $prefetch->{$id} = []
	    }

	undef $ids;

	# now fetch the items

	my $cursor = Tangram::Cursor->new($storage, $ritem, $storage->{db});
	my $includes = $coll->{$member}->includes($ritem);

	# also retrieve collection-side id and index of elmt in sequence
	$cursor->retrieve($coll->{id},
		Tangram::Number->expr("t$includes->{link_tid}.$def->{slot}" ) );

	$cursor->select($filter ? $filter & $includes : $includes);
	
	while (my $item = $cursor->current)
	{
	    my ($coll_id, $slot) = $cursor->residue;
	    $prefetch->{$coll_id}[$slot] = $item;
	    $cursor->next;
	}

    } # skipped
}

$Tangram::Schema::TYPES{hash} = Tangram::Hash->new;

1;
