use strict;

use Tangram::AbstractSet;

package Tangram::Set;

use base qw( Tangram::AbstractSet );

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

      $def->{table} ||= $def->{class} . "_$member";
      $def->{coll} ||= 'coll';
      $def->{item} ||= 'item';
   }
   
   return keys %$members;
}

sub defered_save
{
   my ($self, $storage, $obj, $members, $coll_id) = @_;

   foreach my $member (keys %$members)
   {
      next if tied $obj->{$member};

      my $def = $members->{$member};
      
      my $table = $def->{table} || $def->{class} . "_$member";
      my $coll_col = $def->{coll} || 'coll';
      my $item_col = $def->{item} || 'item';
      
      $self->update($storage, $obj, $member,
         sub
         {
            my $sql = "INSERT INTO $table ($coll_col, $item_col) VALUES ($coll_id, @_)";
            $storage->sql_do($sql);
         },

         sub
         {
            my $sql = "DELETE FROM $table WHERE $coll_col = $coll_id AND $item_col = @_";
            $storage->sql_do($sql);
         } );
   }
}

sub prefetch
{
   my ($self, $storage, $def, $coll, $class, $member, $filter) = @_;
   
   my $ritem = $storage->remote($def->{class});

   my $prefetch = $storage->{PREFETCH}{$class}{$member} ||= {}; # weakref

   my $ids = $storage->my_select_data( cols => [ $coll->{id} ], filter => $filter );

   while (my ($id) = $ids->fetchrow)
   {
      $prefetch->{$id} = []
   }

	my $includes = $coll->{$member}->includes($ritem);
   $includes &= $filter if $filter;

   my $cursor = $storage->my_cursor( $ritem, filter => $includes, retrieve => [ $coll->{id} ] );
   
   while (my $item = $cursor->current)
   {
      my ($coll_id) = $cursor->residue;
      push @{ $prefetch->{$coll_id} }, $item;
      $cursor->next;
   }

   return $prefetch;
}

sub demand
{
   my ($self, $def, $storage, $obj, $member, $class) = @_;

   print $Tangram::TRACE "loading $member\n" if $Tangram::TRACE;

   my $set = Set::Object->new;

   if (my $prefetch = $storage->{PREFETCH}{$class}{$member}{$storage->id($obj)})
   {
      $set->insert(@$prefetch);
   }
   else
   {
      my $cursor = Tangram::CollCursor->new($storage, $def->{class}, $storage->{db});

      my $coll_id = $storage->id($obj);
      my $coll_tid = $storage->alloc_table;
      my $table = $def->{table};
      my $item_tid = $cursor->{-stored}->root_table;
      my $coll_col = $def->{coll} || 'coll';
      my $item_col = $def->{item} || 'item';
      $cursor->{-coll_tid} = $coll_tid;
      $cursor->{-coll_from} = ", $table t$coll_tid";
      $cursor->{-coll_where} = "t$coll_tid.$coll_col = $coll_id AND t$coll_tid.$item_col = t$item_tid.id";

      $set->insert($cursor->select);
   }

   $self->remember_state($def, $storage, $obj, $member, $set);

   $set;
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
	  
	  if ($def->{aggreg})
	  {
		  my @content = $obj->{$member}->members;
		  $storage->sql_do($sql);
		  $storage->erase( @content ) ;
	  }
	  else
	  {
		  $storage->sql_do($sql);
	  }
   }
}

sub query_expr
{
   my ($self, $obj, $members, $tid) = @_;
   map { Tangram::CollExpr->new($obj, $_); } values %$members;
}

$Tangram::Schema::TYPES{set} = Tangram::Set->new;

1;
