# (c) Sound Object Logic 2000-2001

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
	
	$self->update($storage, $obj, $field,
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

sub demand
  {
	my ($self, $def, $storage, $obj, $member, $class) = @_;

	my $set = Set::Object->new();

	if (my $prefetch = $storage->{PREFETCH}{$class}{$member}{$storage->export_object($obj)})
	{
		$set->insert(@$prefetch);
	}
	else
	{
		print $Tangram::TRACE "loading $member\n" if $Tangram::TRACE;

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

		if ($def->{aggreg})
		{
			$storage->erase( $obj->{$member}->members );
		}
		else
		{
			my $item_classdef = $storage->{schema}{classes}{$def->{class}};
			my $table = $item_classdef->{table} || $def->{class};
			my $item_col = $def->{coll};
			$storage->sql_do("UPDATE $table SET $item_col = NULL WHERE $item_col = $coll_id");
		}
	}
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
