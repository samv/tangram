use strict;

package Tangram::Array;

use Tangram::AbstractArray;
use base qw( Tangram::AbstractArray );

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
   }
   
   return keys %$members;
}

sub defered_save
{
   use integer;

   my ($self, $storage, $obj, $members, $coll_id) = @_;

	my $old_states = $storage->{scratch}{ref($self)}{$coll_id};

   foreach my $member (keys %$members)
   {
      next if tied $obj->{$member}; # collection has not been loaded, thus not modified

      my $def = $members->{$member};
      my ($table, $coll_col, $item_col, $slot_col) = @{ $def }{ qw( table coll item slot ) };
      
		my $coll = $obj->{$member};
		my $coll_size = @$coll;

      my $old_state = $old_states->{$member};
      my $old_size = $old_state ? @$old_state : 0;

      my $common_size = $coll_size < $old_size ? $coll_size : $old_size;

		my @new_state = ();
		my $slot = 0;

		while ($slot < $common_size)
		{
			my $item_id = $storage->id($coll->[$slot]) || croak "member $coll->[$slot] has no id";
         my $old_id = $old_state->[$slot];
         
         unless ($item_id == $old_id)
         {
            # array entry has changed value
            my $sql = "UPDATE $table SET $item_col = $item_id WHERE $coll_col = $coll_id AND $slot_col = $slot AND $item_col = $old_id";
            $storage->sql_do($sql);
         }
			
         push @new_state, $item_id;
			++$slot;
		}

      if ($old_size > $coll_size)
		{
         # array shrinks
			my $sql = "DELETE FROM $table WHERE $coll_col = $coll_id AND $slot_col >= $slot";
			$storage->sql_do($sql);
		}

		while ($slot < $coll_size)
		{
         # array grows
			my $item_id = $storage->id($coll->[$slot]) || croak "member $coll->[$slot] has no id";
         my $sql = "INSERT INTO $table ($coll_col, $item_col, $slot_col) VALUES ($coll_id, $item_id, $slot)";
         $storage->sql_do($sql);
			push @new_state, $item_id;
			++$slot;
		}

      $old_states->{$member} = \@new_state;
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

sub cursor
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
   my ($self, $storage, $def, $coll, $class, $member, $filter) = @_;

   my $ritem = $storage->remote($def->{class});

	# first retrieve the collection-side ids of all objects satisfying $filter
	# empty the corresponding prefetch array

   my $ids = $storage->my_select_data( cols => [ $coll->{id} ], filter => $filter );
   my $prefetch = $storage->{PREFETCH}{$class}{$member} ||= {}; # weakref

   while (my ($id) = $ids->fetchrow)
   {
      $prefetch->{$id} = []
   }

   undef $ids;

	# now fetch the items

   my $cursor = Tangram::Cursor->new($storage, $ritem, $storage->{db});
	my $includes = $coll->{$member}->includes($ritem);

	# also retrieve collection-side id and index of elmt in sequence
   $cursor->retrieve($coll->{id},
		Tangram::Expr->new("t$includes->{link_tid}.$def->{slot}", 'Tangram::Number') );

   $cursor->select($filter ? $filter & $includes : $includes);
   
   while (my $item = $cursor->current)
   {
      my ($coll_id, $slot) = $cursor->residue;
      $prefetch->{$coll_id}[$slot] = $item;
      $cursor->next;
   }

   return $prefetch;
}

$Tangram::Schema::TYPES{array} = Tangram::Array->new;

1;
