# (c) Sound Object Logic 2000-2001

use strict;

use Tangram::Type;
use Tangram::Ref;

package Tangram::Coll;

use base qw( Tangram::Type );

sub get_import_cols
  {
	()
  }

sub get_importer
{
  my ($self, $context) = @_;
  my $class = $context->{class}{name};
  my $field = $self->{name};
  
  return sub {
	my ($obj, $row, $context) = @_;
	tie $obj->{$field}, 'Tangram::CollOnDemand', $self, $self, $context->{storage}, $context->{id}, $self->{name}, $class;
	}
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
	my ($self, $storage, $obj, $member) = @_;
	return $storage->{scratch}{ref($self)}{$storage->id($obj)}{$member};
}

sub array_diff
{
	my ($new_state, $old_state, $differ) = @_;

	return (0, []) unless $new_state && $old_state;

	$differ ||= sub { shift() != shift() };

	my $old_size = @$old_state;
	my $new_size = @$new_state;
	my $common = $old_size < $new_size ? $old_size : $new_size;

	use integer;

	my @changed = grep { $differ->($old_state->[$_], $new_state->[$_]) } 0 .. ($common-1);

	return ($common, \@changed);
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
			$target = $coll->{storage}->export_object($item)
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

use overload '<' => \&includes;

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

		$item_id = $storage->export_object($item);

	}
	else
	{
		$item_id = $storage->{export_id}->($item);
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
	my ($self, $template, $cols, $from, $where) = @_;

	push @$where, $self->{-coll_where}
	if $self->{-coll_where};

	push @$cols, $self->{-coll_cols} if exists $self->{-coll_cols};
	push @$from, $self->{-coll_from} if exists $self->{-coll_from};
	
	$self->SUPER::build_select($template, $cols, $from, $where);
}

sub DESTROY
{
	my ($self) = @_;
	#print "@{[ keys %$self ]}\n";
	# $self->{-storage}->free_table($self->{-coll_tid});
}

package Tangram::BackRefOnDemand;

use base qw( Tangram::RefOnDemand );

sub FETCH
{
	my $self = shift;
	my ($storage, $id, $member, $refid, $class, $field) = @$self;
	my $obj = $storage->{objects}{$id};

	my $owner = $storage->remote($class);
	my ($refobj) = $storage->select($owner, $owner->{$field}->includes($obj));
#	my $refobj = $storage->load($refid);

	untie $obj->{$member};
	$obj->{$member} = $refobj;	# weak
	return $refobj;
}

package Tangram::BackRef;

use base qw( Tangram::Scalar );

$Tangram::Schema::TYPES{backref} = Tangram::BackRef->new;

sub get_export_cols
  {
	()
  }

sub get_exporter
  {
  }

sub get_importer
{
  my ($self, $context) = @_;
  my $field = $self->{name};

  return sub {
	my ($obj, $row, $context) = @_;

	my $rid = shift @$row;

	if ($rid) {
	  tie $obj->{$field}, 'Tangram::BackRefOnDemand', $context->{storage}, $context->{id}, $self->{name}, $rid, $self->{class}, $self->{field};
	} else {
	  $obj->{$field} = undef;
	}
  }
}

package Tangram::Alias;

my $top = 1_000;

sub new
{
	'l' . ++$top
}

1;
