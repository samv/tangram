use strict;

use Tangram::AbstractSet;

package Tangram::IntrSet;

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

      $def->{coll} ||= $class . "_$member";

      $schema->{classes}{$def->{class}}{stateless} = 0;

      if (exists $def->{back})
      {
         my $back = $def->{back} ||= $def->{coll};
         $schema->{classes}{ $def->{class} }{members}{backref}{$back} =
	     { col => $def->{coll} };
	 die '***' unless $schema->{types}{backref};
      }
   }
   
   return keys %$members;
}

sub defered_save
{
   my ($self, $storage, $obj, $members, $coll_id) = @_;

	my $classes = $storage->{schema}{classes};

   foreach my $member (keys %$members)
   {
      next if tied $obj->{$member};

      my $def = $members->{$member};
      
      my $item_classdef = $classes->{$def->{class}};
      my $table = $item_classdef->{table};
      my $item_col = $def->{coll};
      
      $self->update($storage, $obj, $member,
         sub
         {
            my $sql = "UPDATE $table SET $item_col = $coll_id WHERE id = @_";
            $storage->sql_do($sql);
         },

         sub
         {
            my $sql = "UPDATE $table SET $item_col = NULL WHERE id = @_ AND $item_col = $coll_id";
            $storage->sql_do($sql);
         } );
   }
}

sub demand
{
   my ($self, $def, $storage, $obj, $member, $class) = @_;

   my $set = Set::Object->new();

   if (my $prefetch = $storage->{PREFETCH}{$class}{$member}{$storage->id($obj)})
   {
      $set->insert(@$prefetch);
   }
   else
   {
      print $Tangram::TRACE "loading $member\n" if $Tangram::TRACE;

      my $cursor = Tangram::CollCursor->new($storage, $def->{class}, $storage->{db});

      my $coll_id = $storage->id($obj);
      my $tid = $cursor->{-stored}->leaf_table;
      $cursor->{-coll_where} = "t$tid.$def->{coll} = $coll_id";
   
      $set->insert($cursor->select);
   }

   $self->remember_state($def, $storage, $obj, $member, $set);

   return $set;
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
      
      my $sql = "UPDATE $table SET $item_col = NULL WHERE $item_col = $coll_id";
      $storage->sql_do($sql);
   }
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

$Tangram::Schema::TYPES{iset} = Tangram::IntrSet->new;

1;
