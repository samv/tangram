# (c) Sound Object Logic 2000-2001

use strict;

package Tangram::Storage;
use DBI;
use Carp;

use vars qw( %storage_class );

BEGIN {

  eval { require 'WeakRef.pm' };

  if ($@) {
    *Tangram::weaken = sub { };
    $Tangram::no_weakrefs = 1;
  } else {
    *Tangram::weaken = \&WeakRef::weaken;
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
	  my $control = $dbh->selectall_arrayref("SELECT major, minor FROM $schema->{control}");

	  if ($control) {
		$self->{class_col} = $schema->{sql}{class_col};
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
	    $self->{ids}{0 + $obj} = $id;
	  } else {
	    delete $self->{ids}{0 + $obj};
	  }
	};

    $self->{get_id} = $schema->{get_id} || sub {
	  my $address = 0 + shift();
	  my $id = $self->{ids}{$address};
	  return undef unless $id;
	  return $id if $self->{objects}{$id};
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
    DBI->connect($self->{-cs}, $self->{-user}, $self->{-pw}) or die;
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
	
	print $Tangram::TRACE "preparing [@{[ $psi++ ]}] $sql\n" if $Tangram::TRACE;
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
	my $id = $sth->fetchrow_arrayref()->[0];
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
		
		foreach my $rollback ( @{ pop @{ $self->{tx} } } )
		  {
			$rollback->($self);
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
	my $cache = $engine->get_save_cache($class);

	my $sths = $self->{INSERT_STHS}{$class_name} ||=
	  [ map { $self->prepare($_) } @{ $cache->{INSERTS} } ];

	my $context = { storage => $self, dbh => $dbh, id => $id, SAVING => $saving };
	my @state = ( $self->{export_id}->($id), $classId, $cache->{EXPORTER}->($obj, $context) );

	my $fields = $cache->{INSERT_FIELDS};

	use integer;

	for my $i (0..$#$sths) {

	  if ($Tangram::TRACE) {
		printf $Tangram::TRACE "executing %s with (%s)\n",
		$cache->{INSERTS}[$i],
		join(', ', map { $_ || 'NULL' } @state[ @{ $fields->[$i] } ] )
	  }

	  my $sth = $sths->[$i];
	  $sth->execute(@state[ @{ $fields->[$i] } ]);
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
	my $dbh = $self->{db};
	my $context = { storage => $self, dbh => $dbh, id => $id, SAVING => $saving };

	my $cache = $self->{engine}->get_save_cache($class);
	my @state = ( $self->{export_id}->($id), substr($id, -$self->{cid_size}), $cache->{EXPORTER}->($obj, $context) );

	my $fields = $cache->{UPDATE_FIELDS};

	my $sths = $self->{UPDATE_STHS}{$class->{name}} ||=
	  [ map {
		print $Tangram::TRACE "preparing $_\n" if $Tangram::TRACE;
		$self->prepare($_)
	  } @{ $cache->{UPDATES} } ];

	use integer;

	for my $i (0..$#$sths) {

	  if ($Tangram::TRACE) {
		printf $Tangram::TRACE "executing %s with (%s)\n",
		$cache->{UPDATES}[$i],
		join(', ', map { $_ || 'NULL' } @state[ @{ $fields->[$i] } ] )
	  }

	  my $sth = $sths->[$i];
	  $sth->execute(@state[ @{ $fields->[$i] } ]);
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
				 [ map { $self->prepare($_) } @{ $self->{engine}->get_deletes($class) } ];
		   
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
    my $prefetch = $self->{prefetch};

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
	$self->{engine}->get_import_cache($self->{schema}->classdef($class))
	  ->($obj, $row, { storage => $self, id => $id, layout1 => $self->{layout1} });
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

sub select
{
    croak "valid only in list context" unless wantarray;

    my ($self, $target, @args) = @_;

    if (ref($target) eq 'ARRAY')
      {
	my ($first, @others) = @$target;

	my @cache = map { $self->select( $_, @args ) } @others;

	my $cursor = Tangram::Cursor->new($self, $first, $self->{db});
	$cursor->retrieve( map { $_->{id} } @others );
	
	my $obj = $cursor->select( @args );
	my @results;

	while ($obj)
	  {
	    push @results, [ $obj, map { $self->load($_) } $cursor->residue() ];
	    $obj = $cursor->next();
	  }

	return @results;
      }
    else
      {
	my $cursor = Tangram::Cursor->new($self, $target, $self->{db});
	$cursor->select(@args);
      }
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
    my ($self, $obj) = @_;
    return $self->{get_id}->($obj);
}

sub disconnect
{
    my ($self) = @_;

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

    my $db = $opts->{dbh} || DBI->connect($cs, $user, $pw);

    eval { $db->{AutoCommit} = 0 };

    $self->{no_tx} = $db->{AutoCommit};

    $self->{db} = $db;

    @$self{ -cs, -user, -pw } = ($cs, $user, $pw);

    $self->{cid_size} = $schema->{sql}{cid_size};
	
    $self->_open($schema);

	$self->{engine} = Tangram::Relational::Engine->new($schema, layout1 => $self->{layout1});

    return $self;
}

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

package Tangram::Relational::TableSet;

use constant TABLES => 0;
use constant SORTED_TABLES => 1;
use constant KEY => 2;

sub new
  {
	my $class = shift;
	my %seen;
	my @tables = grep { !$seen{$_}++ } @_;
	my @sorted_tables = sort @tables;

	return bless [ \@tables, \@sorted_tables, "@sorted_tables" ], $class;
  }

sub key
  {
	return shift->[KEY];
  }

sub tables
  {
	@{ shift->[TABLES] }
  }

sub is_improper_superset
  {
	my ($self, $other) = @_;
	my %other_tables = map { $_ => 1 } $other->tables();
	
	for my $table ($self->tables()) {
	  delete $other_tables{$table};
	  return 1 if keys(%other_tables) == 0;
	}

	return 0;
  }

package Tangram::Relational::Engine;

sub new
  {
	my ($class, $schema, %opts) = @_;

	my $heterogeneity = { };
	my $engine = bless { SCHEMA => $schema,	HETEROGENEITY => $heterogeneity }, $class;

	if ($opts{layout1}) {
	  $engine->{layout1} = 1;
	  $engine->{TYPE_COL} = $schema->{sql}{class_col} || 'classId';
	} else {
	  $engine->{TYPE_COL} = $schema->{sql}{class_col} || 'type';
	}

	for my $class ($schema->all_classes) {
	  $engine->{ROOT_TABLES}{$class->{table}} = 1
		if $class->is_root();
	}

	for my $class ($schema->all_classes) {

	  $engine->{ROOT_TABLES}{$class->{table}} = 1
		if $class->is_root();

	  next if $class->{abstract};

	  my $table_set = $engine->get_table_set($class);
	  my $key = $table_set->key();

	  for my $other ($schema->all_classes) {
		++$heterogeneity->{$key} if my $ss = $engine->get_table_set($other)->is_improper_superset($table_set);
		my $other_key = $engine->get_table_set($other)->key;
	  }
	}

	# use Data::Dumper; print Dumper $heterogeneity;

	return $engine;
  }

sub get_table_set
  {
	my ($self, $class) = @_;

	return $self->{CLASSES}{$class->{name}}{table_set} ||= do {

	  my @table;

	  if ($self->{ROOT_TABLES}{$class->{table}}) {
		push @table, $class->{table};
	  } else {
		my $context = { layout1 => $self->{layout1} };
		
		for my $field ($class->direct_fields()) {
		  if ($field->get_export_cols($context)) {
			push @table, $class->{table};
			last;
		  }
		}
	  }

	  Tangram::Relational::TableSet
		->new((map { $self->get_table_set($_)->tables } $class->direct_bases()), @table );
	};
  }

sub get_grouped_fields
  {
	my ($self, $class) = @_;

	my $cache = $self->{CLASSES}{$class->{name}};

	@{ $self->{CLASSES}{$class->{name}}{grouped_fields} ||= do {
	  my %seen;
	  [ grep { !$seen{$_->[0]{name}}++ }
		(map { $self->get_grouped_fields($_) } $class->direct_bases()),
		[ $class, [ $class->direct_fields() ] ]

	  ]
	} }
  }

sub get_save_cache
  {
	my ($self, $class) = @_;

	return $self->{CLASSES}{$class}{SAVE} ||= do {
	  
	  my $schema = $self->{SCHEMA};
	  my $id_col = $schema->{sql}{id_col};
	  my $type_col = $self->{TYPE_COL};

	  my (%tables, @tables);
	  my (@export_sources, @export_closures);
	  
	  my $context = { layout1 => $self->{layout1} };

	  my $field_index = 2;

	  for my $group ($self->get_grouped_fields($class)) {
		my ($part, $fields) = @$group;
		my $table_name = $part->{table};

		$context->{class} = $part;

		my $table = $tables{$table_name} ||= do { push @tables, my $table = [ $table_name, [], [] ]; $table };
		
		for my $field (@$fields) {
		  
		  my $exporter = $field->get_exporter($context)
			or next;
		  
		  if (ref $exporter) {
			push @export_closures, $exporter;
			push @export_sources, 'shift(@closures)->($obj, $context)';
		  } else {
			push @export_sources, $exporter;
		  }

		  my @export_cols = $field->get_export_cols($context);
		  push @{ $table->[1] }, @export_cols;
		  push @{ $table->[2] }, $field_index..($field_index + $#export_cols);
		  $field_index += @export_cols;
		}
	  }

	  my $export_source = join ",\n", @export_sources;
	  my $copy_closures = @export_closures ? ' my @closures = @export_closures;' : '';

	  # $Tangram::TRACE = \*STDOUT;

	  $export_source = "sub { my (\$obj, \$context) = \@_;$copy_closures\n$export_source }";

	  print $Tangram::TRACE "Compiling exporter for $class->{name}...\n$export_source\n"
		if $Tangram::TRACE;

	  # use Data::Dumper; print Dumper \@cols;
	  my $exporter = eval $export_source or die;

	  my (@inserts, @updates, @insert_fields, @update_fields);

	  for my $table (@tables) {
		my ($table_name, $cols, $fields) = @$table;
		my @meta = ( $id_col );
		my @meta_fields = ( 0 );

		if ($self->{ROOT_TABLES}{$table_name}) {
		  push @meta, $type_col;
		  push @meta_fields, 1;
		}

		next unless @meta > 1 || @$cols;
		
		push @inserts, sprintf('INSERT INTO %s (%s) VALUES (%s)',
								$table_name,
								join(', ', @meta, @$cols),
								join(', ', ('?') x (@meta + @$cols)));
		push @insert_fields, [ @meta_fields, @$fields ];

		if (@$cols) {
		  push @updates, sprintf('UPDATE %s SET %s WHERE %s = %s',
								 $table_name,
								 join(', ', map { "$_ = ?" } @$cols),
								 $id_col, '?');
		  push @update_fields, [ @$fields, 0 ];
		}
	  }

	  {
		EXPORTER => $exporter,
		INSERT_FIELDS => \@insert_fields, INSERTS => \@inserts,
		UPDATE_FIELDS => \@update_fields, UPDATES => \@updates,
	  }
	};
  }

sub get_instance_select
  {
	my ($self, $class) = @_;
	
	return $self->{CLASSES}{$class}{INSTANCE_SELECT} ||= do {
	  my $schema = $self->{SCHEMA};
	  my $id_col = $schema->{sql}{id_col};
	  my $context = { engine => $self, schema => $schema, layout1 => $self->{layout1} };
	  my @cols;
	  
	  for my $group ($self->get_grouped_fields($class)) {
		my ($part, $fields) = @$group;
		my $table = $part->{table};
		$context->{class} = $part;
		push @cols, map { "$table.$_" } map { $_->get_import_cols($context) } @$fields;
	  }

	  my ($first_table, @other_tables) = $self->get_table_set($class)->tables();

	  sprintf("SELECT %s FROM %s WHERE %s",
			  join(', ', @cols),
			  join(', ', $first_table, @other_tables),
			  join(' AND ', "$first_table.$id_col = ?", map { "$first_table.$id_col = $_.$id_col" } @other_tables));
	};
  }

sub get_polymorphic_select
  {
	my ($self, $class, $storage) = @_;
	
	my $selects = $self->{CLASSES}{$class}{POLYMORPHIC_SELECT} ||= do {
	  my $schema = $self->{SCHEMA};
	  my $id_col = $schema->{sql}{id_col};
	  my $type_col = $self->{TYPE_COL};
	  my $context = { engine => $self, schema => $schema, layout1 => $self->{layout1} };
	  
	  my $table_set = $self->get_table_set($class);
	  my %base_tables = do { my $ph = 0; map { $_ => $ph++ } $table_set->tables() };
	  
	  my %partition;
	  
	  $class->for_conforming(sub {
							   my $class = shift;
							   push @{ $partition{ $self->get_table_set($class)->key } }, $class
								 unless $class->{abstract};
							 } );
	  
	  my @selects;
	  
	  for my $table_set_key (keys %partition) {

		my $mates = $partition{$table_set_key};
		
		my %slice;
		my %col_index;
		my $col_mark = 0;
		my (@cols, @expand);
		
		my @tables = $self->get_table_set($mates->[0])->tables();
		
		my $root_table = $tables[0];
		push @cols, qualify($id_col, $root_table, \%base_tables, \@expand);
		push @cols, qualify($type_col, $root_table, \%base_tables, \@expand);
		
		my %used;
		$used{$root_table} += 2;

		for my $class (@$mates) {
		  my @slice;
		  
		  for my $group ($self->get_grouped_fields($class)) {
			my ($part, $fields) = @$group;
			my $table = $part->{table};
			$context->{class} = $part;
			
			for my $field (@$fields) {
			  my @import_cols = $field->get_import_cols($context);
			  $used{$table} += @import_cols;

			  for my $col (@import_cols) {
				push @slice, $col_index{$col} ||= $col_mark++;
				push @cols, qualify($col, $table, \%base_tables, \@expand);
			  }
			}
		  }
		  
		  $slice{ $storage->{class2id}{$class->{name}} || $class->{id} } = \@slice; # should be $class->{id} (compat)
		}
		
		my @from;
		
		for my $table (@tables) {
		  next unless $used{$table};
		  if (exists $base_tables{$table}) {
			push @expand, $base_tables{$table};
			push @from, "$table t%d";
		  } else {
			push @from, $table;
		  }
		}
		
		my @where = map {
		  qualify($id_col, $root_table, \%base_tables, \@expand) . ' = ' . qualify($id_col, $_, \%base_tables, \@expand)
		} grep { $used{$_} } @tables[1..$#tables];

		unless (@$mates == $self->{HETEROGENEITY}{$table_set_key}) {
		  push @where, sprintf "%s IN (%s)", qualify($type_col, $root_table, \%base_tables, \@expand),
		  join ', ', map {
			$storage->{class2id}{$_->{name}} or $_->{id} # try $storage first for compatibility with layout1
		  } @$mates
		}
		
		push @selects, Tangram::Relational::PolySelectTemplate->new(\@expand, \@cols, \@from, \@where, \%slice);
	  }

	  \@selects;
	};

	return @$selects;
  }

sub qualify
  {
	my ($col, $table, $ph, $expand) = @_;
	
	if (exists $ph->{$table}) {
	  push @$expand, $ph->{$table};
	  return "t%d.$col";
	} else {
	  return "$table.$col";
	}
  }

sub get_import_cache
  {
    my ($self, $class) = @_;

	return $self->{CLASSES}{$class}{IMPORTER} ||=
	  do {
		my $schema = $self->{SCHEMA};
		
		my $context = { schema => $schema, layout1 => $self->{layout1} };
		
		my (@import_sources, @import_closures);
		
		for my $group ($self->get_grouped_fields($class)) {
		  my ($part, $fields) = @$group;
		  my $table_name = $part->{table};
		  
		  $context->{class} = $part;
		  
		  for my $field (@$fields) {
			
			my $importer = $field->get_importer($context)
			  or next;
			
			if (ref $importer) {
			  push @import_closures, $importer;
			  push @import_sources, 'shift(@closures)->($obj, $row, $context)';
			} else {
			  push @import_sources, $importer;
			}
		  }
		}
		
		my $import_source = join ";\n", @import_sources;
		my $copy_closures = @import_closures ? ' my @closures = @import_closures;' : '';
		
		# $Tangram::TRACE = \*STDOUT;
		
		$import_source = "sub { my (\$obj, \$row, \$context) = \@_;$copy_closures\n$import_source }";
		
		print $Tangram::TRACE "Compiling importer for $class->{name}...\n$import_source\n"
		  if $Tangram::TRACE;
		
		# use Data::Dumper; print Dumper \@cols;
		eval $import_source or die;
	  };
  }

sub get_deletes
  {
	my ($self, $class) = @_;
	
	return $self->{CLASSES}{$class}{DELETES} ||= do {
	  my $id_col = $self->{SCHEMA}{sql}{id_col};
	  [ map { "DELETE FROM $_ WHERE $id_col = ?" } $self->get_table_set($class)->tables() ]
	};
  }

package Tangram::Relational::PolySelectTemplate;

sub new
  {
	my $class = shift;
	bless [ @_ ], $class;
  }

sub instantiate
  {
	my ($self, $remote, $xcols, $xfrom, $xwhere) = @_;
	my ($expand, $cols, $from, $where) = @$self;

	$xcols ||= [];
	$xfrom ||= [];
	$xwhere ||= [];

	my @tables = $remote->table_ids();

	my $select = sprintf "SELECT %s\n  FROM %s", join(', ', @$cols, @$xcols), join(', ', @$from, @$xfrom);

	$select = sprintf "%s\n  WHERE %s", $select, join(' AND ', @$where, @$xwhere)
	  if @$where || @$xwhere;

	sprintf $select, map { $tables[$_] } @$expand;
  }

sub extract
{
  my ($self, $row) = @_;
  my $id = shift @$row;
  my $class_id = shift @$row;
  my $slice = $self->[-1]{$class_id} or Carp::croak "unexpected class id '$class_id'";
  my $state = [ @$row[ @$slice ] ];
  splice @$row, 0, @{ $self->[1] } - 2;
  return ($id, $class_id, $state);
}	

1;
