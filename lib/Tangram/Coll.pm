

package Tangram::Coll;
use strict;

use Tangram::Expr::Coll;
use Tangram::Expr::Coll::FromMany;
use Tangram::Expr::Coll::FromOne;
use Tangram::Lazy::Coll;

use Tangram::Expr::LinkTable;

use Tangram::Type;
use Tangram::Ref;

use vars qw(@ISA);
 @ISA = qw( Tangram::Type );

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
	tie $obj->{$field}, 'Tangram::Lazy::Coll', $self, $self, $context->{storage}, $context->{id}, $self->{name}, $class;
	}
}

sub read
{
	my ($self, $row, $obj, $members, $storage, $class) = @_;

	foreach my $member (keys %$members)
	{
		tie $obj->{$member}, 'Tangram::Lazy::Coll',
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

use vars qw(@ISA);
 @ISA = qw( Tangram::RefOnDemand );

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

use vars qw(@ISA);
 @ISA = qw( Tangram::Scalar );

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
