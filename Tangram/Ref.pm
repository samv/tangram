use strict;

package Tangram::RefOnDemand;

sub TIESCALAR
{
   my $pkg = shift;
   return bless [ @_ ], $pkg;
}

sub FETCH
{
   my $self = shift;
   my ($storage, $id, $member, $refid) = @$self;
   my $obj = $storage->{objects}{$id};
   my $refobj = $storage->load($refid);
   untie $obj->{$member};
   $obj->{$member} = $refobj;
   return $refobj;
}

sub STORE
{
   my ($self, $val) = @_;
   my ($storage, $id, $member, $refid) = @$self;
   my $obj = $storage->{objects}{$id};
   untie $obj->{$member};
   return $obj->{$member} = $val;
}

sub id
{
   my ($storage, $id, $member, $refid) = @{shift()};
   $refid
}

use Tangram::Scalar;

package Tangram::Ref;

use base qw( Tangram::Scalar );

$Tangram::Schema::TYPES{ref} = Tangram::Ref->new;

sub save
{
   my ($self, $cols, $vals, $obj, $members, $storage, $table, $id) = @_;

   foreach my $member (keys %$members)
   {
      push @$cols, $members->{$member}{col};
      my $tied = tied($obj->{$member});
      push @$vals, $tied ? $tied->id : $storage->auto_insert($obj->{$member}, $table, $member, $id);
   }
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
         tie $obj->{$r}, 'Tangram::RefOnDemand', $storage, $id, $r, $rid;
      }
      else
      {
         $obj->{$r} = undef;
      }
   }
}

sub query_expr
{
   my ($self, $obj, $memdefs, $tid, $storage) = @_;
   my $dialect = $storage->{dialect};
   return map { $self->expr("t$tid.$_", $obj) } keys %$memdefs;
}

sub refid
{
   my ($storage, $obj, $member) = @_;
   
   Carp::carp "Tangram::Ref::refid( \$storage, \$obj, \$member )" unless !$^W
      && eval { $storage->isa('Tangram::Storage') }
      && eval { $obj->isa('UNIVERSAL') }
      && !ref($member);

   my $tied = tied($obj->{$member});
   
   return $storage->id( $obj->{$member} ) unless $tied;

   my ($storage_, $id_, $member_, $refid) = @$tied;
   return $refid;
}

sub erase
{
	my ($self, $storage, $obj, $members) = @_;

	foreach my $member (keys %$members)
	{
		$storage->erase( $obj->{$member} )
			if $members->{$member}{aggreg} && $obj->{$member};
	}
}

1;

