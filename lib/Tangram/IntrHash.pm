# (c) Sound Object Logic 2000-2001

# not implemented yet

package Tangram::IntrHash;

use strict;
use vars qw(@ISA);
BEGIN { @ISA = qw( Tangram::AbstractHash ) };

use Carp;

sub reschema {
    my ($self, $members, $class, $schema) = @_;
   
    foreach my $member (keys %$members) {
	my $def = $members->{$member};

	unless (ref($def))
	    {
		$def = { class => $def };
		$members->{$member} = $def;
	    }

	$def->{coll} ||= $schema->{normalize}->($class) . "_$member";
	$def->{slot} ||=($schema->{normalize}->($class) . "_$member"
			 . "_slot");
   
	$schema->{classes}{$def->{class}}{stateless} = 0;
	if (exists $def->{back}) {
	    my $back = $def->{back} ||= $def->{item};
	    $schema->{classes}{ $def->{class} }{members}{backref}{$back} =
		bless {
		       name => $back,
		       col => $def->{coll},
		       class => $class,
		       field => $member
		      }, 'Tangram::BackRef';
	}
    }

    return keys %$members;
}

sub defered_save
{
   use integer;

   my ($self, $obj, $field, $storage) = @_;
   return if tied $obj->{$field};

   my $coll_id = $storage->export_object($obj);

   my $classes = $storage->{schema}{classes};
   my $def = $self;  # surely!

   my $old_states = $storage->{scratch}{ref($self)}{$field};
   my $item_classdef = $classes->{$def->{class}};

   # get the schema definition for the collection
   my $table = $item_classdef->{table} or die;
   my $item_col = $def->{coll};
   my $slot_col = $def->{slot};

   my $coll = $obj->{$field};

   my %new_state = ();
   my $old_state = $old_states->{$field} || {};

   my %removed = %$old_state;

   my $slot = 0;

   while (my $slot = each %$coll) {

       my $item_id = $storage->export_object( $coll->{$slot} ) || die;

       $storage->sql_do("UPDATE\n    $table\nSET\n    $item_col = $coll_id,\n    $slot_col = ?\nWHERE\n    $storage->{schema}{sql}{id_col} = ?", $slot, $item_id)
	   unless (exists $old_state->{$slot} and
		   $item_id eq $old_state->{$slot});

       $new_state{$slot} = $item_id;
       delete $removed{$slot};
   }

   if (keys %removed)
       {
	   my $removed = join(' ', values %removed);
	   $storage->sql_do("UPDATE\n    $table\nSET\n    $item_col = NULL,\n    $slot_col = NULL\nWHERE\n    $storage->{schema}{sql}{id_col} IN ($removed)");
       }

   $old_states->{$field} = \%new_state;

   $storage->tx_on_rollback( sub { $old_states->{$field} = $old_state } );
}

sub erase
{
   my ($self, $storage, $obj, $members, $coll_id) = @_;

   foreach my $member (keys %$members)
   {
      next if tied $obj->{$member};

      my $def = $members->{$member};
      my $item_classdef = $storage->{schema}{classes}{$def->{class}};
      my $table = $item_classdef->{table} || $def->{class};
      my $item_col = $def->{coll};
      my $slot_col = $def->{slot};
      
      my $sql = "UPDATE\n    $table\nSET\n    $item_col = NULL,\n    $slot_col = NULL\nWHERE\n    $item_col = $coll_id";
      $storage->sql_do($sql);
   }
}

sub cursor
{
   my ($self, $def, $storage, $obj, $member) = @_;

   my $cursor = Tangram::CollCursor->new($storage, $def->{class}, $storage->{db});

   my $item_col = $def->{coll};
   my $slot_col = $def->{slot};

   my $coll_id = $storage->export_object($obj);
   my $tid = ${ $cursor }{ TARGET }->object->{table_hash}{$def->{class}}
       ; # ->leaf_table;
   
   $cursor->{-coll_cols} = "t$tid.$slot_col";
   $cursor->{-coll_where} = "t$tid.$item_col = $coll_id";

   return $cursor;
}

sub query_expr
{
   my ($self, $obj, $members, $tid) = @_;
   map { Tangram::IntrCollExpr->new($obj, $_); } values %$members;
}

sub remote_expr
{
   my ($self, $obj, $tid) = @_;
   Tangram::IntrCollExpr->new($obj, $self);
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

   $cursor->retrieve
       ($coll->{id},
	$storage->expr(Tangram::Scalar->instance,
		       "t$ritem->{_object}{table_hash}{$def->{class}}"
		       .".$def->{slot}")
       );

   $cursor->select($includes);

   while (my $item = $cursor->current)
   {
      my ($coll_id, $slot) = $cursor->residue;
      $prefetch->{$coll_id}{$slot} = $item;
      $cursor->next;
   }
}

$Tangram::Schema::TYPES{ihash} = Tangram::IntrHash->new;

1;
