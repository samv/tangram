use strict;

use Tangram::Type;
use Tangram::Ref;

package Tangram::Coll;

use base qw( Tangram::Type );

sub members
{
	my ($self, $members) = @_;
	keys %$members;
}

sub cols
{
	()
}

sub read
{
	my ($self, $row, $obj, $members, $storage, $class) = @_;

	foreach my $member (keys %$members)
	{
		tie $obj->{$member}, 'Tangram::CollOnDemand',
			$self, $members->{$member}, $storage, $storage->id($obj), $member, $class;
	}
}

sub bad_type
{
	my ($obj, $coll, $class, $item) = @_;
    die "$item is not a '$class' in collection '$coll' of $obj";
}

sub set_load_state
{
	my ($self, $storage, $obj, $member, $state) = @_;
	$storage->{scratch}{ref($self)}{$storage->id($obj)}{$member} = $state;
}

sub get_load_state
{
	my ($self, $storage, $obj, $member, $state) = @_;
	return $storage->{scratch}{ref($self)}{$storage->id($obj)}{$member};
}

package Tangram::AbstractCollExpr;

sub new
{
	my $pkg = shift;
	bless [ @_ ], $pkg;
}

sub exists
{
	my ($self, $expr, $filter) = @_;
	my ($coll) = @$self;

	if ($expr->isa('Tangram::QueryObject'))
	{
		$expr = Tangram::Select->new
			(
			 cols => [ $expr->{id} ],
			 exclude => [ $coll ],
			 filter => $self->includes($expr)->and_perhaps($filter)
			);
	}

	my $expr_str = $expr->{expr};
	$expr_str =~ tr/\n/ /;

	return Tangram::Filter->new( expr => "exists $expr_str", tight => 100,
								 objects => Set::Object->new( $expr->objects() ) );
}

package Tangram::CollExpr;

use base qw( Tangram::AbstractCollExpr );

sub includes
{
	my ($self, $item) = @_;
	my ($coll, $memdef) = @$self;

	my $coll_tid = $coll->root_table;

	my $link_tid = Tangram::Alias->new;
	my $coll_col = $memdef->{coll};
	my $item_col = $memdef->{item};

	my $objects = Set::Object->new($coll, Tangram::LinkTable->new($memdef->{table}, $link_tid) ),
		my $target;

	if (ref $item)
	{
		if ($item->isa('Tangram::QueryObject'))
		{
			$target = 't' . $item->object->root_table . '.id';
			$objects->insert( $item->object );
		}
		else
		{
			$target = $coll->{storage}->id($item)
				or die "'$item' is not a persistent object";
		}
	}
	else
	{
		$target = $item;
	}

	Tangram::Filter->new
			(
			 expr => "t$link_tid.$coll_col = t$coll_tid.id AND t$link_tid.$item_col = $target",
			 tight => 100,      
			 objects => $objects,
			 link_tid => $link_tid # for Sequence prefetch
			);
}

package Tangram::IntrCollExpr;

use base qw( Tangram::AbstractCollExpr );

sub includes
{
	my ($self, $item) = @_;
	my ($coll, $memdef) = @$self;
	my $coll_tid = $coll->root_table;
	my $item_class = $memdef->{class};
	my $storage = $coll->{storage};

	my $item_id;

	if (ref($item))
	{
		if ($item->isa('Tangram::QueryObject'))
		{
			my $item_tid = $item->object->table($item_class);

			return Tangram::Filter->new
				(
				 expr => "t$item_tid.$memdef->{coll} = t$coll_tid.id",
				 tight => 100,
				 objects => Set::Object->new($coll, $item->object),
				)
			}

		$item_id = $storage->id($item);

	}
	else
	{
		$item_id = $item;
	}

	my $remote = $storage->remote($item_class);
	return $self->includes($remote) & $remote->{id} == $item_id;
}

package Tangram::LinkTable;
use Carp;

sub new
{
	my ($pkg, $name, $alias) = @_;
	bless [ $name, $alias ], $pkg;
}

sub from
{
	my ($name, $alias) = @{shift()};
	"$name t$alias"
}

sub where
{
	confess unless wantarray;
	()
}
package Tangram::CollOnDemand;

sub TIESCALAR
{
	my $pkg = shift;
	return bless [ @_ ], $pkg;	# [ $type, $storage, $id, $member, $class ]
}

sub FETCH
{
	my $self = shift;
	my ($type, $def, $storage, $id, $member, $class) = @$self;
	my $obj = $storage->{objects}{$id} or die;
	my $coll = $type->demand($def, $storage, $obj, $member, $class);
	untie $obj->{$member};
	$obj->{$member} = $coll;
}

sub STORE
{
	my ($self, $coll) = @_;
	my ($type, $def, $storage, $id, $member, $class) = @$self;

	my $obj = $storage->{objects}{$id} or die;
	$type->demand($def, $storage, $obj, $member, $class);

	untie $obj->{$member};

	$obj->{$member} = $coll;
}

package Tangram::CollCursor;

@Tangram::CollCursor::ISA = 'Tangram::Cursor';

sub build_select
{
	my ($self, $cols, $from, $where) = @_;

	if ($self->{-coll_where})
	{
		$where .= ' AND ' if $where;
		$where .= "$self->{-coll_where}" if $self->{-coll_where};
	}

	$where = $where && "WHERE $where";
	$cols .= $self->{-coll_cols} if exists $self->{-coll_cols};
	$from .= $self->{-coll_from} if exists $self->{-coll_from};
	"SELECT $cols\n\tFROM $from\n\t$where";
}

sub DESTROY
{
	my ($self) = @_;
	#print "@{[ keys %$self ]}\n";
	$self->{-storage}->free_table($self->{-coll_tid});
}

package Tangram::BackRefOnDemand;

use base qw( Tangram::RefOnDemand );

sub FETCH
{
	my $self = shift;
	my ($storage, $id, $member, $refid) = @$self;
	my $obj = $storage->{objects}{$id};
	my $refobj = $storage->load($refid);
	untie $obj->{$member};
	$obj->{$member} = $refobj;	# weak
	return $refobj;
}

package Tangram::BackRef;

use base qw( Tangram::Scalar );

$Tangram::Schema::TYPES{backref} = Tangram::BackRef->new;

sub save
{
	() # do nothing; save is done by the collection
}

sub read
{
	my ($self, $row, $obj, $members, $storage) = @_;
   
	my $id = $storage->id($obj);

	foreach my $r (keys %$members)
	{
		my $rid = shift @$row;

		if ($rid)
		{
			tie $obj->{$r}, 'Tangram::BackRefOnDemand', $storage, $id, $r, $rid;
		}
		else
		{
			$obj->{$r} = undef;
		}
	}
}

package Tangram::Alias;

my $top = 1_000;

sub new
{
	++$top
}

1;
