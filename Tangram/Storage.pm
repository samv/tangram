# (c) Sound Object Logic 2000-2001

use strict;

package Tangram::Storage;
use DBI;
use Carp;
use Tangram::Core qw(pretty);

use vars qw( %storage_class );

BEGIN {

    eval 'use Scalar::Util qw(refaddr)';
    if ($@) {
	*Tangram::weaken = sub { };
	$Tangram::no_weakrefs = 1;
    } else {
	*Tangram::weaken = \&Scalar::Util::weaken;
	$Tangram::no_weakrefs = 0;
    }
}

sub new
{
    my $pkg = shift;
    return bless { @_ }, $pkg;
}

sub schema
{
    shift->{schema}
}

sub export_object
  {
    my ($self, $obj) = @_;
    return $self->{export_id}->($self->{get_id}->($obj));
  }

sub split_id
  {
	carp unless wantarray;
	my ($self, $id) = @_;
	my $cid_size = $self->{cid_size};
	return ( substr($id, 0, -$cid_size), substr($id, -$cid_size) );
  }

sub combine_ids
  {
	my $self = shift;
	return $self->{layout1} ? shift : sprintf("%d%0$self->{cid_size}d", @_);
  }

sub _open
  {
    my ($self, $schema) = @_;

	my $dbh = $self->{db};

    $self->{table_top} = 0;
    $self->{free_tables} = [];

    $self->{tx} = [];

    $self->{schema} = $schema;

	{
	  local $dbh->{PrintError} = 0;
	  my $control = $dbh->selectall_arrayref("SELECT * FROM $schema->{control}");

	  $self->{id_col} = $schema->{sql}{id_col};

	  if ($control) {
		$self->{class_col} = $schema->{sql}{class_col} || 'type';
		$self->{import_id} = sub { shift() . sprintf("%0$self->{cid_size}d", shift()) };
		$self->{export_id} = sub { substr shift(), 0, -$self->{cid_size} };
	  } else {
		$self->{class_col} = 'classId';
		$self->{layout1} = 1;
		$self->{import_id} = sub { shift() };
		$self->{export_id} = sub { shift() };
	  }
	}

	my %id2class;

	if ($self->{layout1}) {
	  # compatibility with version 1.x
	  %id2class = map { @$_ } @{ $self->{db}->selectall_arrayref("SELECT classId, className FROM $schema->{class_table}") };
	} else {
	  my $classes = $schema->{classes};
	  %id2class = map { $classes->{$_}{id}, $_ } keys %$classes;
	}

	$self->{id2class} = \%id2class;
	@{ $self->{class2id} }{ values %id2class } = keys %id2class;

    $self->{set_id} = $schema->{set_id} ||
      sub
	{
	  my ($obj, $id) = @_;

	  if ($id) {
	    $self->{ids}{refaddr($obj)} = $id;
	  } else {
	    delete $self->{ids}{refaddr($obj)};
	  }
	};

    $self->{get_id} = $schema->{get_id} || sub {
	  my $address = refaddr(shift());
	  my $id = $self->{ids}{$address};
	  return undef unless $id;
	  return $id if exists $self->{objects}{$id};
	  delete $self->{ids}{$address};
	  delete $self->{objects}{$id};
	  return undef;
	};

    return $self;
  }

sub alloc_table
{
    my ($self) = @_;

    return @{$self->{free_tables}} > 0
	? pop @{$self->{free_tables}}
	    : ++$self->{table_top};
}

sub free_table
{
    my $self = shift;
    push @{$self->{free_tables}}, grep { $_ } @_;
}

sub open_connection
{
    # private - open a new connection to DB for read

    my $self = shift;
    my $db = DBI->connect($self->{-cs}, $self->{-user}, $self->{-pw})
	or die;

    if (exists $self->{no_tx}) {
	$db->{AutoCommit} = ($self->{no_tx} ? 1 : 0);

    }

    return $db;
}

sub close_connection
  {
    # private - close read connection to DB unless it's the default one
	
    my ($self, $conn) = @_;

	return unless $conn &&  $self->{db};
	
    if ($conn == $self->{db})
	  {
		$conn->commit unless $self->{no_tx} || @{ $self->{tx} };
	  }
    else
	  {
		$conn->disconnect;
	  }
  }

sub cursor
{
    my ($self, $class, @args) = @_;
    my $cursor = Tangram::Cursor->new($self, $class, $self->open_connection());
    $cursor->select(@args);
    return $cursor;
}

sub my_cursor
{
    my ($self, $class, @args) = @_;
    my $cursor = Tangram::Cursor->new($self, $class, $self->{db});
    $cursor->select(@args);
    return $cursor;
}

sub select_data
{
    my $self = shift;
    Tangram::Select->new(@_)->execute($self, $self->open_connection());
}

sub selectall_arrayref
{
    shift->select_data(@_)->fetchall_arrayref();
}

sub my_select_data
{
    my $self = shift;
    Tangram::Select->new(@_)->execute($self, $self->{db});
}

my $psi = 1;

sub prepare
  {
	my ($self, $sql) = @_;
	
	print $Tangram::TRACE "Tangram::Storage: "
	    ."preparing [@{[ $psi++ ]}] $sql\n"
	    if $Tangram::TRACE && ($Tangram::DEBUG_LEVEL > 1);
	$self->{db}->prepare($sql);
  }

*prepare_insert = \&prepare;
*prepare_update = \&prepare;
*prepare_select = \&prepare;

sub make_id
  {
    my ($self, $class_id) = @_;
	
	unless ($self->{layout1}) {
	  my $id;
	  
	  if (exists $self->{mark}) {
		$id = $self->{mark}++;
		$self->{set_mark} = 1;	# cleared by tx_start
	  } else {
		$id = $self->make_1st_id_in_tx();
	  }
	  
	  return sprintf "%d%0$self->{cid_size}d", $id, $class_id;
	}

	# ------------------------------
	# compatibility with version 1.x

    my $alloc_id = $self->{alloc_id} ||= {};
    
    my $id = $alloc_id->{$class_id};
    
    if ($id)      {
		$id = -$id if $id < 0;
		$alloc_id->{$class_id} = ++$id;
      } else {
		my $table = $self->{schema}{class_table};
		$self->sql_do("UPDATE $table SET lastObjectId = lastObjectId + 1 WHERE classId = $class_id");
		$id = $self
		  ->sql_selectall_arrayref("SELECT lastObjectId from $table WHERE classId = $class_id")->[0][0];
		$alloc_id->{$class_id} = -$id;
      }
    
    return sprintf "%d%0$self->{cid_size}d", $id, $class_id;
  }

sub make_1st_id_in_tx
  {
    my ($self) = @_;
    
	unless ($self->{make_id}) {
	  my $table = $self->{schema}{control};
	  my $dbh = $self->{db};
	  $self->{make_id}{inc} = $self->prepare("UPDATE $table SET mark = mark + 1");
	  $self->{make_id}{set} = $self->prepare("UPDATE $table SET mark = ?");
	  $self->{make_id}{get} = $self->prepare("SELECT mark from $table");
	}
	
	my $sth;
	
	$sth = $self->{make_id}{inc};
	$sth->execute();
	$sth->finish();
	
	$sth = $self->{make_id}{get};
	$sth->execute();
    my $row = $sth->fetchrow_arrayref() or
	die "`Tangram' table corrupt; insert a valid row!";
	my $id = $row->[0];
    while ($row =  $sth->fetchrow_arrayref()) {
	warn "Eep!  More than one row in `Tangram' table!";
	$id = $row->[0] if ($row->[0] > $id);
    }
	$sth->finish();

	return $id;
  }

sub update_id_in_tx
  {
	my ($self, $mark) = @_;
	my $sth = $self->{make_id}{set};
	$sth->execute($mark);
	$sth->finish();
  }

sub unknown_classid
{
    my $class = shift;
    confess "class '$class' doesn't exist in this storage"
}

sub class_id
{
    my ($self, $class) = @_;
    $self->{class2id}{$class} or unknown_classid $class;
}

#############################################################################
# Transaction

my $error_no_transaction = 'no transaction is currently active';

sub tx_start
{
    my $self = shift;

	unless (@{ $self->{tx} }) {
	  delete $self->{set_mark};
	  delete $self->{mark};
	}

    push @{ $self->{tx} }, [];
}

sub tx_commit
  {
    # public - commit current transaction
    
    my $self = shift;
    
    carp $error_no_transaction unless @{ $self->{tx} };
    
    # update lastObjectId's
    
    if ($self->{set_mark}) {
	  $self->update_id_in_tx($self->{mark});
	}

	# ------------------------------
	# compatibility with version 1.x

    if (my $alloc_id = $self->{alloc_id}) {
	  my $table = $self->{schema}{class_table};
	
	  for my $class_id (keys %$alloc_id)
		{
		  my $id = $alloc_id->{$class_id};
		  next if $id < 0;
		  $self->sql_do("UPDATE $table SET lastObjectId = $id WHERE classId = $class_id");
		}
	  
	  delete $self->{alloc_id};
	}
	
	# compatibility with version 1.x
	# ------------------------------
    
    unless ($self->{no_tx} || @{ $self->{tx} } > 1) {
	  # committing outer tx: commit to db
	  $self->{db}->commit;
	}
	
    pop @{ $self->{tx} };		# drop rollback subs
  }

sub tx_rollback
  {
    my $self = shift;
    
    carp $error_no_transaction unless @{ $self->{tx} };
    
    if ($self->{no_tx})
      {
		pop @{ $self->{tx} };
      }
    else
      {
		$self->{db}->rollback if @{ $self->{tx} } == 1; # don't rollback db if nested tx
		
		# execute rollback subs in reverse order

		if (my $rb = pop @{ $self->{tx} }) {
		    foreach my $rollback ( @$rb )
			{
			    $rollback->($self);
			}
		}
	  }
}

sub tx_do
{
    # public - execute subroutine inside tx

    my ($self, $sub, @params) = @_;

    $self->tx_start();

    my ($results, @results);
    my $wantarray = wantarray();

    eval
    {
		if ($wantarray)
		{
			@results = $sub->(@params);
		}
		else
		{
			$results = $sub->(@params);
		}
    };

    if ($@)
    {
		$self->tx_rollback();
		die $@;
    }
    else
    {
		$self->tx_commit();
    }

    return wantarray ? @results : $results;
}

sub tx_on_rollback
{
    # private - register a sub that will be called if/when the tx is rolled back

    my ($self, $rollback) = @_;
    carp $error_no_transaction if $^W && !@{ $self->{tx} };
    unshift @{ $self->{tx}[0] }, $rollback; # rollback subs are executed in reverse order
}

#############################################################################
# insertion

sub insert
{
    # public - insert objects into storage; return their assigned ids

    my ($self, @objs) = @_;

    my @ids = $self->tx_do(
	   sub
	   {
		   my ($self, @objs) = @_;
		   map
		   {
			   local $self->{defered} = [];
			   my $id = $self->_insert($_, Set::Object->new() );
			   $self->do_defered;
			   $id;
		   } @objs;
	   }, $self, @objs );

    return wantarray ? @ids : shift @ids;
}

sub _insert
{
    my ($self, $obj, $saving) = @_;

	die unless $saving;

    my $schema = $self->{schema};

    return $self->id($obj)
      if $self->id($obj);

    $saving->insert($obj);

    my $class_name = ref $obj;
    my $classId = $self->{class2id}{$class_name} or unknown_classid $class_name;
	my $class = $self->{schema}->classdef($class_name);

    my $id = $self->make_id($classId);

    $self->welcome($obj, $id);
    $self->tx_on_rollback( sub { $self->goodbye($obj, $id) } );

	my $dbh = $self->{db};
	my $engine = $self->{engine};

	my $sths = $self->{INSERT_STHS}{$class_name} ||=
	  [ map { $self->prepare($_) } $engine->get_insert_statements($class) ];

	my $context = { storage => $self, dbh => $dbh, id => $id, SAVING => $saving };
	my @state = ( $self->{export_id}->($id), $classId, $class->get_exporter({layout1 => $self->{layout1} })->($obj, $context) );

	my @fields = $engine->get_insert_fields($class);

	use integer;

	for my $i (0..$#$sths) {

	  if ($Tangram::TRACE) {
		my @sql = $engine->get_insert_statements($class);
		printf $Tangram::TRACE "executing %s with (%s)\n",
		$sql[$i],
		join(', ', map { $_ || 'NULL' } @state[ @{ $fields[$i] } ] )
	  }

	  my $sth = $sths->[$i];
	  $sth->execute(@state[ @{ $fields[$i] } ]);
	  $sth->finish();
	}

    return $id;
  }

#############################################################################
# update

sub update
{
    # public - write objects to storage

    my ($self, @objs) = @_;

    $self->tx_do(
		 sub
		 {
		     my ($self, @objs) = @_;
		     foreach my $obj (@objs)
		     {
			   local $self->{defered} = [];

			   $self->_update($obj, Set::Object->new() );
			   $self->do_defered;
		     }
		   }, $self, @objs);
  }

sub _update
  {
    my ($self, $obj, $saving) = @_;

	die unless $saving;

    my $id = $self->id($obj) or confess "$obj must be persistent";

    $saving->insert($obj);

    my $class = $self->{schema}->classdef(ref $obj);
	my $engine = $self->{engine};
	my $dbh = $self->{db};
	my $context = { storage => $self, dbh => $dbh, id => $id, SAVING => $saving };

	my @state = ( $self->{export_id}->($id), substr($id, -$self->{cid_size}), $class->get_exporter({ layout1 => $self->{layout1} })->($obj, $context) );
	my @fields = $engine->get_update_fields($class);

	my $sths = $self->{UPDATE_STHS}{$class->{name}} ||=
	  [ map {
		print $Tangram::TRACE "Tangram::Storage: preparing $_\n"
		    if ( $Tangram::TRACE && ( $Tangram::DEBUG_LEVEL > 1 ) );
		$self->prepare($_)
	  } $engine->get_update_statements($class) ];

	use integer;

	for my $i (0..$#$sths) {

	  if ($Tangram::TRACE) {
		my @sql = $engine->get_update_statements($class);
		printf $Tangram::TRACE "executing %s with (%s)\n",
		$sql[$i],
		join(', ', map { $_ || 'NULL' } @state[ @{ $fields[$i] } ] )
	  }

	  my $sth = $sths->[$i];
	  $sth->execute(@state[ @{ $fields[$i] } ]);
	  $sth->finish();
	}
  }

#############################################################################
# save

sub save
  {
    my $self = shift;
	
    foreach my $obj (@_) {
	  if ($self->id($obj)) {
	    $self->update($obj)
	  }	else {
	    $self->insert($obj)
	  }
    }
  }

sub _save
  {
	my ($self, $obj, $saving) = @_;
	
	if ($self->id($obj)) {
	  $self->_update($obj, $saving)
	} else {
	  $self->_insert($obj, $saving)
	}
  }


#############################################################################
# erase

sub erase
  {
    my ($self, @objs) = @_;

    $self->tx_do(
		 sub
		 {
		   my ($self, @objs) = @_;
		   my $schema = $self->{schema};
		   my $classes = $self->{schema}{classes};

		   foreach my $obj (@objs)
		     {
		       my $id = $self->id($obj) or confess "object $obj is not persistent";
			   my $class = $schema->classdef(ref $obj);

		       local $self->{defered} = [];
			   
		       $schema->visit_down(ref($obj),
					   sub
					   {
					     my $class = shift;
					     my $classdef = $classes->{$class};

					     foreach my $typetag (keys %{$classdef->{members}}) {
					       my $members = $classdef->{members}{$typetag};
					       my $type = $schema->{types}{$typetag};
					       $type->erase($self, $obj, $members, $id);
					     }
					   } );

			   my $sths = $self->{DELETE_STHS}{$class->{name}} ||=
				 [ map { $self->prepare($_) } $self->{engine}->get_deletes($class) ];
		   
		       my $eid = $self->{export_id}->($id);

			   for my $sth (@$sths) {
				 $sth->execute($eid);
				 $sth->finish();
			   }

		       $self->do_defered;

		       $self->goodbye($obj, $id);
		       $self->tx_on_rollback( sub { $self->welcome($obj, $id) } );
		     }
		 }, $self, @objs );
  }

sub do_defered
{
    my ($self) = @_;

    foreach my $defered (@{$self->{defered}})
    {
		$defered->($self);
    }

    $self->{defered} = [];
}

sub defer
{
    my ($self, $action) = @_;
    push @{$self->{defered}}, $action;
}

sub load
{
    my $self = shift;

    return map { scalar $self->load( $_ ) } @_ if wantarray;

    my $id = shift;
    die if @_;

    return $self->{objects}{$id}
      if exists $self->{objects}{$id} && defined $self->{objects}{$id};

    my $class = $self->{schema}->classdef( $self->{id2class}{ int(substr($id, -$self->{cid_size})) } );

	my $row = _fetch_object_state($self, $id, $class);

    my $obj = $self->read_object($id, $class->{name}, $row);

    # ??? $self->{-residue} = \@row;

    return $obj;
}

sub reload
{
    my $self = shift;

    return map { scalar $self->load( $_ ) } @_ if wantarray;

	my $obj = shift;
    my $id = $self->id($obj) or die "'$obj' is not persistent";
    my $class = $self->{schema}->classdef( $self->{id2class}{ int(substr($id, -$self->{cid_size})) } );

	my $row = _fetch_object_state($self, $id, $class);
    _row_to_object($self, $obj, $id, $class->{name}, $row);

    return $obj;
}

sub welcome
  {
    my ($self, $obj, $id) = @_;
    $self->{set_id}->($obj, $id);

    Tangram::weaken( $self->{objects}{$id} = $obj );
  }

sub goodbye
  {
    my ($self, $obj, $id) = @_;
    $self->{set_id}->($obj, undef) if $obj;
    delete $self->{objects}{$id};
    delete $self->{PREFETCH}{$id};
  }

sub shrink
  {
    my ($self) = @_;

    my $objects = $self->{objects};
    my $prefetch = $self->{PREFETCH};

    for my $id (keys %$objects)
      {
	next if $objects->{$id};
	delete $objects->{$id};
	delete $prefetch->{$id};
      }
  }

sub read_object
  {
    my ($self, $id, $class, $row, @parts) = @_;

    my $schema = $self->{schema};

    my $obj = $schema->{make_object}->($class);

    unless (exists $self->{objects}{$id} && defined $self->{objects}{$id}) {
      # do this only if object is not loaded yet
      # otherwise we're just skipping columns in $row
      $self->welcome($obj, $id);
    }

    _row_to_object($self, $obj, $id, $class, $row, @parts);

    return $obj;
  }

sub _row_to_object
  {
    my ($self, $obj, $id, $class, $row) = @_;
	my $context = { storage => $self, id => $id, layout1 => $self->{layout1} };
	$self->{schema}->classdef($class)->get_importer($context)->($obj, $row, $context);
    if (my $x=$obj->can("T2_import")) {
	$x->($obj);
    }
	return $obj;
}

sub _fetch_object_state
{
    my ($self, $id, $class) = @_;

	my $sth = $self->{LOAD_STH}{$class->{name}} ||=
	  $self->prepare($self->{engine}->get_instance_select($class));

	$sth->execute($self->{export_id}->($id));
	my $state = [ $sth->fetchrow_array() ];
	$sth->finish();

	return $state;
}

sub get_polymorphic_select
  {
	my ($self, $class) = @_;
	return $self->{engine}->get_polymorphic_select($self->{schema}->classdef($class), $self);
  }

sub select {
  croak "valid only in list context" unless wantarray;
  
  my ($self, $target, @args) = @_;
  
  unless (ref($target) eq 'ARRAY') {
	my $cursor = Tangram::Cursor->new($self, $target, $self->{db});
	return $cursor->select(@args);
  }
  
  my ($first, @others) = @$target;
  
  my @cache = map { $self->select( $_, @args ) } @others;
  
  my $cursor = Tangram::Cursor->new($self, $first, $self->{db});
  $cursor->retrieve( map { $_->{_IID_}, $_->{_TYPE_ } } @others );
  
  my $obj = $cursor->select( @args );
  my @results;
  
  while ($obj) {
	my @tuple = $obj;
	my @residue = $cursor->residue;
	
	while (my $id = shift @residue) {
	  push @tuple, $self->load($self->combine_ids($id, shift @residue));
	}
	
	push @results, \@tuple;
	$obj = $cursor->next;
  }
  
  return @results;
}

sub cursor_object
  {
    my ($self, $class) = @_;
    $self->{IMPLICIT}{$class} ||= Tangram::RDBObject->new($self, $class)
}

sub query_objects
{
    my ($self, @classes) = @_;
    map { Tangram::QueryObject->new(Tangram::RDBObject->new($self, $_)) } @classes;
}

sub remote
{
    my ($self, @classes) = @_;
    wantarray ? $self->query_objects(@classes) : (&remote)[0]
}

sub expr
  {
    my $self = shift;
    return shift->expr( @_ );
  }

sub object
{
    carp "cannot be called in list context; use objects instead" if wantarray;
    my $self = shift;
    my ($obj) = $self->query_objects(@_);
    $obj;
}

sub count
{
    my $self = shift;

    my ($target, $filter);
    my $objects = Set::Object->new;

    if (@_ == 1)
    {
	$target = '*';
	$filter = shift;
    }
    else
    {
	my $expr = shift;
	$target = $expr->{expr};
	$objects->insert($expr->objects);
	$filter = shift;
    }

    my @filter_expr;

    if ($filter)
    {
	$objects->insert($filter->objects);
	@filter_expr = ( "($filter->{expr})" );
    }

    my $sql = "SELECT COUNT($target) FROM " . join(', ', map { $_->from } $objects->members);
   
    $sql .= "\nWHERE " . join(' AND ', @filter_expr, map { $_->where } $objects->members);

    print $Tangram::TRACE "$sql\n" if $Tangram::TRACE;

    return ($self->{db}->selectrow_array($sql))[0];
}

sub sum
{
    my ($self, $expr, $filter) = @_;

    my $objects = Set::Object->new($expr->objects);

    my @filter_expr;

    if ($filter)
    {
	$objects->insert($filter->objects);
	@filter_expr = ( "($filter->{expr})" );
    }

    my $sql = "SELECT SUM($expr->{expr}) FROM " . join(', ', map { $_->from } $objects->members);

    $sql .= "\nWHERE " . join(' AND ', @filter_expr, map { $_->where } $objects->members);

    print $Tangram::TRACE "$sql\n" if $Tangram::TRACE;

    return ($self->{db}->selectrow_array($sql))[0];
}

sub id
{
    my $self = shift;
	return map { $self->{get_id}->($_) } @_ if wantarray;
    $self->{get_id}->(shift());
}

sub disconnect
{
    my ($self) = @_;

    return unless defined $self->{db};

    unless ($self->{no_tx})
    {   
	if (@{ $self->{tx} })
	{
	    $self->{db}->rollback;
	}
	else
	{
	    $self->{db}->commit;
	}
    }
   
    $self->{db}->disconnect;

    %$self = ();
}

sub _kind_class_ids
{
    my ($self, $class) = @_;

    my $schema = $self->{schema};
    my $classes = $self->{schema}{classes};
    my $class2id = $self->{class2id};

    my @ids;

    push @ids, $self->class_id($class) unless $classes->{$class}{abstract};

    $schema->for_each_spec($class,
			   sub { my $spec = shift; push @ids, $class2id->{$spec} unless $classes->{$spec}{abstract} } );

    return @ids;
}

sub is_persistent
{
    my ($self, $obj) = @_;
    return $self->{schema}->is_persistent($obj) && $self->id($obj);
}

sub prefetch
{
	my ($self, $remote, $member, $filter) = @_;

	my $class;

	if (ref $remote)
	{
		$class = $remote->class();
	}
	else
	{
		$class = $remote;
		$remote = $self->remote($class);
	}

	my $schema = $self->{schema};

	my $member_class = $schema->find_member_class($class, $member)
		or die "no member '$member' in class '$class'";

	my $classdef = $schema->{classes}{$member_class};
	my $type = $classdef->{member_type}{$member};
	my $memdef = $classdef->{MEMDEFS}{$member};

	$type->prefetch($self, $memdef, $remote, $class, $member, $filter);
}

sub connect
{
    my ($pkg, $schema, $cs, $user, $pw, $opts) = @_;

    my $self = $pkg->new;

	$opts ||= {};

    @$self{ -cs, -user, -pw } = ($cs, $user, $pw);

    my $db = $opts->{dbh} || $self->open_connection;
 
	if (exists $opts->{no_tx}) {
	  $self->{no_tx} = $opts->{no_tx};
	} else {
	  eval { $db->{AutoCommit} = 0 };
	  $self->{no_tx} = $db->{AutoCommit};
	}

    if (exists $opts->{no_subselects}) {
	$self->{no_subselects} = $opts->{no_subselects};
    } else {
	local($SIG{__WARN__})=sub{};
	eval {
	    my $sth = $db->prepare("select * from (select 1+1)");
	    $sth->execute() or die;
	};
	if ($@ or $DBI::errstr) {
	    $self->{no_subselects} = 1;
	}
    }

    $self->{db} = $db;

    $self->{cid_size} = $schema->{sql}{cid_size};
	
    $self->_open($schema);

	$self->{engine} = Tangram::Relational::Engine->new($schema, layout1 => $self->{layout1});

    return $self;
}

sub connection { shift->{db} }

sub sql_do
{
    my ($self, $sql) = @_;
    print $Tangram::TRACE "$sql\n" if $Tangram::TRACE;
	my $rows_affected = $self->{db}->do($sql);
    return defined($rows_affected) ? $rows_affected
	  : croak $DBI::errstr;
}

sub sql_selectall_arrayref
{
    my ($self, $sql, $dbh) = @_;
    print $Tangram::TRACE "$sql\n" if $Tangram::TRACE;
	($dbh || $self->{db})->selectall_arrayref($sql);
}

sub sql_prepare
{
    my ($self, $sql, $connection) = @_;
    confess unless $connection;
    print $Tangram::TRACE "$sql\n" if $Tangram::TRACE;
    return $connection->prepare($sql) or die;
}

sub sql_cursor
{
    my ($self, $sql, $connection) = @_;

    confess unless $connection;

    print $Tangram::TRACE "$sql\n" if $Tangram::TRACE;

    my $sth = $connection->prepare($sql) or die;
    $sth->execute() or confess;

    Tangram::Storage::Statement->new( statement => $sth, storage => $self,
				     connection => $connection );
}

sub unload
  {
    my $self = shift;
    my $objects = $self->{objects};

    if (@_) {
      for my $item (@_) {
	if (ref $item) {
	  $self->goodbye($item, $self->{get_id}->($item));
	} else {
	  $self->goodbye($objects->{$item}, $item);
	}
      }
    } else {
      for my $id (keys %$objects) {
	$self->goodbye($objects->{$id}, $id);
      }
    }
  }

# checks to see if an object ID ->isa the correct type, based on its
# classtype
sub oid_isa
    {
	my $self = shift;
	my $oid = shift;
	croak(pretty($oid)." is not an Object ID")
	    unless defined ($oid) and $oid + 0 eq $oid;

	my $class = shift;
	my $classes = $self->{schema}->{classes};
	croak "Class ".pretty($oid)." is not defined in the schema"
	    unless defined($class) and exists $classes->{$class};

	my @bases = $self->{id2class}->{ ($self->split_id($oid))[1] + 0 };

	my $seen = Set::Object->new();
	while (my $base = shift @bases) {
	    $seen->insert($classes->{$base}) or next;
	    return 1 if $base eq $class;
	    push @bases, @{ $classes->{$base}->{bases} }
		if exists $classes->{$base}->{bases};
	}

	return undef;
    }

*reset = \&unload; # deprecated, use unload() instead

sub DESTROY
{
    my $self = shift;
    $self->{db}->disconnect if $self->{db};
}

package Tangram::Storage::Statement;

sub new
{
    my $class = shift;
    bless { @_ }, $class;
}

sub fetchrow
{
    return shift->{statement}->fetchrow;
}

sub close
{
    my $self = shift;

    if ($self->{storage})
    {
	$self->{statement}->finish;
	$self->{storage}->close_connection($self->{connection});
	%$self = ();
    }
}

1;
