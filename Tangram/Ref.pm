# (c) Sound Object Logic 2000-2001

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
   my $refobj;
   if ($id) {
       my $obj = $storage->{objects}{$id};
       $refobj = $storage->load($refid);
       untie $obj->{$member};
       $obj->{$member} = $refobj;
   } else {
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

use Tangram::Scalar;

package Tangram::Ref;

use vars qw(@ISA);
 @ISA = qw( Tangram::Scalar );

$Tangram::Schema::TYPES{ref} = Tangram::Ref->new;

sub field_reschema
  {
	my ($self, $field, $def, $schema) = @_;
	$self->SUPER::field_reschema($field, $def, $schema);
	die unless $field;
	$def->{type_col} ||= $schema->{normalize}->("${field}_type", "colname");
  }

sub get_export_cols
{
    my ($self, $context) = @_;
	return $context->{layout1} ? ( $self->{col} ) : ( $self->{col}, $self->{type_col} );
}

sub get_import_cols
{
    my ($self, $context) = @_;
	return $context->{layout1} ? ( $self->{col} ) : ( $self->{col}, $self->{type_col} );
}

sub get_exporter
  {
	my ($self, $context) = @_;

	my $field = $self->{name};
	my $table = $context->{class}{table};
	my $deep_update = $self->{deep_update};
	
	if ($context->{layout1}) {
	  return sub {
		my ($obj, $context) = @_;
		
		return undef unless exists $obj->{$field};
		
		my $storage = $context->{storage};
		
		my $tied = tied($obj->{$field});
		return $tied->id if $tied;
		
		my $ref = $obj->{$field};
		return undef unless $ref;
		
		my $id = $storage->id($obj);
		
		if ($context->{SAVING}->includes($ref)) {
		  $storage->defer( sub
						   {
							 my $storage = shift;
							 
							 # now that the object has been saved, we have an id for it
							 my $refid = $storage->id($ref);
							 # patch the column in the referant
							 $storage->sql_do( "UPDATE $table SET $self->{col} = $refid WHERE id = $id" );
						   } );
		  
		  return undef;
		}
		
		$storage->_save($ref, $context->{SAVING})
		  if $deep_update;
		
		return $storage->id($ref) || $storage->_insert($ref, $context->{SAVING});
	  }
	}
	
	return sub {
	  
	  my ($obj, $context) = @_;
	  
	  return (undef, undef) unless exists $obj->{$field};
	  
	  my $storage = $context->{storage};
	  
	  my $tied = tied($obj->{$field});
	  return $storage->split_id($tied->id) if $tied;
	  
	  my $ref = $obj->{$field};
	  return (undef, undef) unless $ref;
	  
	  my $exp_id = $storage->export_object($obj);
	  
	  if ($context->{SAVING}->includes($ref)) {
		$storage->defer( sub
						 {
						   my $storage = shift;
						   
						   # now that the object has been saved, we have an id for it
						   my $ref_id = $storage->export_object($ref);
						   my $type_id = $storage->class_id(ref($ref));
						   
						   # patch the column in the referant
						   $storage->sql_do( "UPDATE $table SET $self->{col} = $ref_id, $self->{type_col} = $type_id WHERE id = $exp_id" );
						 } );
		
		return (undef, undef);
	  }
	  
	  $storage->_save($ref, $context->{SAVING})
		if $deep_update;
	  
	  return $storage->split_id($storage->id($ref) || $storage->_insert($ref, $context->{SAVING}));
	}
  }

sub get_importer
{
  my ($self, $context) = @_;
  my $field = $self->{name};

  return sub {
	my ($obj, $row, $context) = @_;
	
	my $storage = $context->{storage};
	my $rid = shift @$row;
	my $cid = shift @$row unless $context->{layout1};

	if ($rid) {
	  tie $obj->{$field}, 'Tangram::RefOnDemand', $storage, $context->{id}, $field, $storage->combine_ids($rid, $cid);
	} else {
	  $obj->{$field} = undef;
	}
  }
}

sub query_expr
{
   my ($self, $obj, $memdefs, $tid, $storage) = @_;
   return map { $self->expr("t$tid.$memdefs->{$_}{col}", $obj) } keys %$memdefs;
}

sub remote_expr
{
   my ($self, $obj, $tid, $storage) = @_;
   $self->expr("t$tid.$self->{col}", $obj);
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

sub coldefs
{
    my ($self, $cols, $members, $schema) = @_;

    for my $def (values %$members) {
	  my $nullable = !exists($def->{null}) || $def->{null} ? " $schema->{sql}{default_null}" : '';
	  $cols->{ $def->{col} } = $schema->{sql}{id} . $nullable;
	  $cols->{ $def->{type_col} or die } = $schema->{sql}{cid} . $nullable;
    }
}

sub DESTROY { }

1;

