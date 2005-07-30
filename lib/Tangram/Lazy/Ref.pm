
package Tangram::Lazy::Ref;

use Tangram::Scalar;
use strict;

sub TIESCALAR
{
   my $pkg = shift;
   return bless [ @_ ], $pkg;
}

sub FETCH
{
   my $self = shift;
   my ($storage, $id, $member, $refid) = @$self;
   my $refobj;

   if ($id) {
       print $Tangram::TRACE "demanding $id.$member".(defined $storage->{objects}{$refid}
						      ? " (hot)":"")."\n"
	   if $Tangram::TRACE;
       my $obj = $storage->{objects}{$id};
       $refobj = $storage->load($refid);
       untie $obj->{$member};
       $obj->{$member} = $refobj;
   } else {
       print $Tangram::TRACE "demanding obj $refid".(defined $storage->{objects}{$refid}
						      ? " (hot)":"")."\n"
	   if $Tangram::TRACE;
       untie $$member;
       $refobj = $$member = $storage->load($refid);
   }
   return $refobj;
}

sub STORE
{
   my ($self, $val) = @_;
   my ($storage, $id, $member, $refid) = @$self;
   if ($id) {
       my $obj = $storage->{objects}{$id};
       untie $obj->{$member};
       return $obj->{$member} = $val;
   } else {
       untie $$member;
       $$member = $val;
   }
}

sub id
{
   my ($storage, $id, $member, $refid) = @{shift()};
   $refid;
}


