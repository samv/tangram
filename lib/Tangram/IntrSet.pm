

use strict;

use Tangram::AbstractSet;

package Tangram::IntrSet;

use vars qw(@ISA);
 @ISA = qw( Tangram::AbstractSet );

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

		$def->{coll} ||= $schema->{normalize}->($class) . "_$member";

		$schema->{classes}{$def->{class}}{stateless} = 0;

		if (exists $def->{back})
		{
			my $back = $def->{back} ||= $def->{coll};
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
	my ($self, $storage, $obj, $field, $def) = @_;

	return  if tied $obj->{$field};

	my $coll_id = $storage->export_object($obj);
	my $classes = $storage->{schema}{classes};
	my $item_classdef = $classes->{$def->{class}};
	my $table = $item_classdef->{table};
	my $item_col = $def->{coll};

	$self->update
	    ($storage, $obj, $field,
	     sub
	     {
		 if ( $storage->can("t2_insert_hook") ) {
		     $storage->t2_insert_hook( ref($obj), $coll_id, $field, $_[1] );
		 }

		 my $sql = ("UPDATE\n    $table\nSET\n    "
			    ."$item_col = $coll_id\nWHERE\n    "
			    ."$storage->{schema}{sql}{id_col} = $_[0]");
		 $storage->sql_do($sql);
	     },

	     sub
	     {
		 if ( $storage->can("t2_remove_hook") ) {
		     $storage->t2_remove_hook( ref($obj), $coll_id, $field, $_[1] );
		 }

		 if ($def->{aggreg}) {
		     my $id = shift;
		     my $oid = shift;
		     print $Tangram::TRACE "Tangram::IntrSet: removing oid $oid\n"
			 if $Tangram::TRACE;
		     # FIXME - use dummy object
		     $storage->erase( $storage->load( $oid ));
		 } else {
		     my $sql = ("UPDATE\n    $table\nSET\n    "
				."$item_col = NULL\nWHERE\n    "
				."$storage->{schema}{sql}{id_col} = "
				."$_[0]    AND\n    $item_col = $coll_id");
		     $storage->sql_do($sql);
		 }
	     }
	    );
  }

sub demand
  {
	my ($self, $def, $storage, $obj, $member, $class) = @_;

	my $set = Set::Object->new();

	if (my $prefetch = $storage->{PREFETCH}{$class}{$member}{$storage->export_object($obj)})
	{
	    print $Tangram::TRACE "demanding ".$storage->id($obj)
		.".$member from prefetch\n" if $Tangram::TRACE;
		$set->insert(@$prefetch);
	}
	else
	{
	    print $Tangram::TRACE "demanding ".$storage->id($obj)
		.".$member from storage\n" if $Tangram::TRACE;

		my $cursor = Tangram::CollCursor->new($storage, $def->{class}, $storage->{db});

		my $coll_id = $storage->export_object($obj);
		my $tid = $cursor->{TARGET}->object->{table_hash}{$def->{class}}; # leaf_table;
		$cursor->{-coll_where} = "t$tid.$def->{coll} = $coll_id";
   
		$set->insert($cursor->select);
	}

	$self->remember_state($def, $storage, $obj, $member, $set);

	return $set;
}

sub erase
{
	my ($self, $storage, $obj, $members, $coll_id) = @_;

	$coll_id = $storage->{export_id}->($coll_id);

	foreach my $member (keys %$members)
	{
		my $def = $members->{$member};

		if ( $storage->can("t2_remove_hook") ) {
		    $storage->t2_remove_hook
			(
			 ref($obj),
			 $coll_id,
			 $member,
			 (map { $storage->export_object($_) }
			  $obj->{$member}->members),
			);
		}

		if ($def->{aggreg})
		{
			$storage->erase( $obj->{$member}->members );
		}
		else
		{
		    my $item_classdef = $storage->{schema}{classes}{$def->{class}};
		    my $table = $item_classdef->{table} || $def->{class};
		    my $item_col = $def->{coll};
		    $storage->sql_do("UPDATE\n    $table\nSET\n    $item_col = NULL\nWHERE\n    $item_col = $coll_id");
		}
	}
}

sub query_expr
{
	my ($self, $obj, $members, $tid) = @_;
	map { Tangram::Expr::Coll::FromOne->new($obj, $_); } values %$members;
}->new($obj, $_); } values %$members;
}

sub remote_expr
{
	my ($self, $obj, $tid) = @_;
	Tangram::Expr::Coll::FromOne->new($obj, $_); } values %$members;
}->new($obj, $self);
}

sub prefetch
{
	my ($self, $storage, $def, $coll, $class, $member, $filter) = @_;

	my $ritem = $storage->remote($def->{class});

	my $prefetch = $storage->{PREFETCH}{$class}{$member} ||= {}; # weakref

	my $includes = $coll->{$member}->includes($ritem);
	$includes &= $filter if $filter;

	my $cursor = $storage->my_cursor( $ritem, filter => $includes, retrieve => [ $coll->{id} ] );
   
	while (my $item = $cursor->current)
	{
		my ($coll_id) = $cursor->residue;
		push @{ $prefetch->{$coll_id}||=[] }, $item;
		$cursor->next;
	}

	return $prefetch;
}

sub get_intrusions {
  my ($self, $context) = @_;
  return [ $self->{class}, $context->{mapping}->get_home_table($self->{class}) ];
}

$Tangram::Schema::TYPES{iset} = Tangram::IntrSet->new;

1;
