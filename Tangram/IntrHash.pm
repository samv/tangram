# (c) Sound Object Logic 2000-2001

# not implemented yet

__END__

package Tangram::IntrHash;

use base qw( Tangram::AbstractHash );

use Carp;

sub reschema
{
   my ($self, $members, $class, $schema) = @_;

   foreach my $member (keys %$members)
   {
      my $def = $members->{$member};

      unless (ref($def))
      {
         $def = { class => $def };
         $members->{$member} = $def;
      }

      $def->{coll} ||= $class . "_$member";
      $def->{slot} ||= $class . "_$member" . "_slot";
   
      $schema->{classes}{$def->{class}}{stateless} = 0;
   }

   return keys %$members;
}

sub defered_save
{
   use integer;

   my ($self, $storage, $obj, $members, $coll_id) = @_;

	my $classes = $storage->{schema}{classes};
	my $old_states = $storage->{scratch}{ref($self)}{$coll_id};

   foreach my $member (keys %$members)
   {
      next if tied $obj->{$member};
		next unless exists $obj->{$member} && defined $obj->{$member};

      my $def = $members->{$member};
      my $item_classdef = $classes->{$def->{class}};
      my $table = $item_classdef->{table} or die;
      my $item_col = $def->{coll};
      my $slot_col = $def->{slot};

		my $coll_id = $storage->id($obj);
		my $coll = $obj->{$member};
		my $coll_size = @$coll;
		
		my @new_state = ();
		
		my $old_state = $old_states->{$member};
      my $old_size = $old_state ? @$old_state : 0;

		my %removed;
		@removed{ @$old_state } = () if $old_state;

		my $slot = 0;

		while ($slot < $coll_size)
		{
			my $item_id = $storage->id( $coll->[$slot] ) || die;

			$storage->sql_do("UPDATE $table SET $item_col = $coll_id, $slot_col = $slot WHERE id = $item_id")
				unless $slot < $old_size && $item_id eq $old_state->[$slot];

			push @new_state, $item_id;
			delete $removed{$item_id};
			++$slot;
		}

		if (keys %removed)
		{
			my $removed = join(' ', keys %removed);
			$storage->sql_do("UPDATE $table SET $item_col = NULL, $slot_col = NULL WHERE id IN ($removed)");
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
      next if tied $obj->{$member};

      my $def = $members->{$member};
      my $item_classdef = $storage->{schema}{$def->{class}};
      my $table = $item_classdef->{table} || $def->{class};
      my $item_col = $def->{coll};
      my $slot_col = $def->{slot};
      
      my $sql = "UPDATE $table SET $item_col = NULL, $slot_col = NULL WHERE $item_col = $coll_id";
      $storage->sql_do($sql);
   }
}

sub cursor
{
   my ($self, $def, $storage, $obj, $member) = @_;

   my $cursor = Tangram::CollCursor->new($storage, $def->{class}, $storage->{db});

   my $item_col = $def->{coll};
   my $slot_col = $def->{slot};

   my $coll_id = $storage->id($obj);
   my $tid = $cursor->{-stored}->leaf_table;
   $cursor->{-coll_cols} = ", t$tid.$slot_col";
   $cursor->{-coll_where} = "t$tid.$item_col = $coll_id";

   return $cursor;
}

sub query_expr
{
   my ($self, $obj, $members, $tid) = @_;
   map { Tangram::IntrCollExpr->new($obj, $_); } values %$members;
}

sub prefetch
{
   my ($self, $storage, $def, $coll, $class, $member, $filter) = @_;

   my $ritem = $storage->remote($def->{class});

   my $prefetch = $storage->{PREFETCH}{$class}{$member} ||= {}; # weakref

   my $cursor = Tangram::Cursor->new($storage, $ritem, $storage->{db});
	
   my $includes = $coll->{$member}->includes($ritem);
   $includes &= $filter if $filter;

	# also retrieve collection-side id and index of elmt in sequence

   $cursor->retrieve($coll->{id},
		Tangram::Expr->new("t$ritem->{object}{table_hash}{$def->{class}}.$def->{slot}", Tangram::Integer->instance() );

   $cursor->select($includes);
   
   while (my $item = $cursor->current)
   {
      my ($coll_id, $slot) = $cursor->residue;
      $prefetch->{$coll_id}[$slot] = $item;
      $cursor->next;
   }
}

$Tangram::Schema::TYPES{iarray} = Tangram::IntrHash->new;

1;
