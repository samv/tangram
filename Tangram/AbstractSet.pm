# (c) Sound Object Logic 2000-2001

use strict;

use Tangram::Coll;

package Tangram::AbstractSet;

use vars qw(@ISA);
 @ISA = qw( Tangram::Coll );

use Carp;

# Support for classes that lazily create Set::Objects for instance vars.
# -- ks.perl@kurtstephens.com 2004/03/30
sub __lazy_members
{
  $_[0] ? $_[0]->members : ();
}


sub get_exporter
  {
	my ($self, $context) = @_;
	my $field = $self->{name};
	
	return $self->{deep_update} ?
	  sub {
		my ($obj, $context) = @_;
		
		# has collection been loaded? if not, then it hasn't been modified
		return if tied $obj->{$field};
		
		my $storage = $context->{storage};
		
		foreach my $item ( __lazy_members($obj->{$field}) ) {
		  $storage->_save($item, $context->{SAVING});
		}
		
		$storage->defer(sub { $self->defered_save(shift, $obj, $field, $self) } );
		
		return ();
	  }
	: sub {
	  my ($obj, $context) = @_;
	  
	  # has collection been loaded? if not, then it hasn't been modified
	  return if tied $obj->{$field};
	  
	  my $storage = $context->{storage};
	  
	  if (my $s = $obj->{$field}) {
	      if (!UNIVERSAL::isa($s, "Set::Object")) {
		  die "Data error in ${obj}"."->{$field}; expected "
		      ."Set, got $s"
	      } else {
		  foreach my $item ( $s->members ) {
		      $storage->insert($item)
			  unless $storage->id($item);
		  }
	      }
	  }
	  
	  $storage->defer(sub { $self->defered_save(shift, $obj, $field, $self) } );
	  
	  return ();
	}
  }

sub update
{
	my ($self, $storage, $obj, $member, $insert, $remove) = @_;

	return unless defined $obj->{$member};

	my $coll_id = $storage->id($obj);
	my $old_states = $storage->{scratch}{ref($self)}{$coll_id};
	my $old_state = $old_states->{$member};
	my %new_state = ();

	foreach my $item ( __lazy_members($obj->{$member}) )
	{
		my $item_id = $storage->id($item) || croak "member $item has no id";
      
		unless (exists $old_state->{$item_id})
		{
			$insert->($storage->{export_id}->($item_id));
		}

		$new_state{$item_id} = 1;
	}

	foreach my $del (keys %$old_state)
	{
		next if $new_state{$del};
		$remove->($storage->{export_id}->($del));
	}

	$old_states->{$member} = \%new_state;
	$storage->tx_on_rollback( sub { $old_states->{$member} = $old_state } );
}

sub remember_state
{
	my ($self, $def, $storage, $obj, $member, $set) = @_;

	my %new_state;
	for my $member ( __lazy_members($set) ) {
	    my $id = $storage->id($member);
	    $id && ($new_state{ $id } = 1);
	}

	$storage->{scratch}{ref($self)}{$storage->id($obj)}{$member}
	    = \%new_state;
}

sub content
{
	shift;
	__lazy_members(shift); #?#?
}

1;
