use strict;

package Tangram::AbstractStorage;

use Carp;

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

use vars qw( %done );

sub new
{
    my $pkg = shift;
    return bless { @_ }, $pkg;
}

sub schema
{
    shift->{schema}
}

sub _open
  {
    my ($self, $schema) = @_;

    $self->{table_top} = 0;
    $self->{free_tables} = [];

    $self->{tx} = [];

    $self->{schema} = $schema;

    my $cursor = $self->sql_cursor("SELECT classId, className FROM $schema->{class_table}", $self->{db});

    my $classes = $schema->{classes};

    my $id2class = {};
    my $class2id = {};

    my ($classId, $className);

    while (($classId, $className) = $cursor->fetchrow()) {
      $id2class->{$classId} = $className;
      $class2id->{$className} = $classId;
    }

    $cursor->close();

    $self->{id2class} = $id2class;
    $self->{class2id} = $class2id;

    foreach my $class (keys %$classes) {
      warn "no class id for '$class'\n"
	if $classes->{$class}{concrete} && !exists $self->{class2id}{$class};
    }

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

    $self->{get_id} = $schema->{get_id}
      || sub { $self->{ids}{0 + shift()} };

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
    confess unless $conn;

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

sub prepare
  {
	my ($self, $sql) = @_;
	print $Tangram::TRACE "preparing $sql\n" if $Tangram::TRACE;
	$self->{db}->prepare($sql);
  }

*prepare_insert = \&prepare;
*prepare_update = \&prepare;
*prepare_select = \&prepare;

sub make_id
  {
    my ($self, $class_id) = @_;
    
    my $alloc_id = $self->{alloc_id} ||= {};
    
    my $id = $alloc_id->{$class_id};
    
    if ($id)
      {
		$id = -$id if $id < 0;
		$alloc_id->{$class_id} = ++$id;
      }
    else
      {
		$id = $self->make_1st_id_in_tx($class_id);
		$alloc_id->{$class_id} = -$id;
      }
    
    return sprintf "%d%0$self->{cid_size}d", $id, $class_id;
}

sub make_1st_id_in_tx
  {
    my ($self, $class_id) = @_;
    
	unless ($self->{make_id}) {
	  my $table = $self->{schema}{class_table};
	  my $dbh = $self->{db};
	  $self->{make_id}{update} = $self->prepare("UPDATE $table SET lastObjectId = lastObjectId + 1 WHERE classId = ?");
	  $self->{make_id}{select} = $self->prepare("SELECT lastObjectId from $table WHERE classId = ?");
	}
	
	my $sth;
	
	$sth = $self->{make_id}{update};
	$sth->execute($class_id);
	$sth->finish();
	
	$sth = $self->{make_id}{select};
	$sth->execute($class_id);
	my $id = $sth->fetchrow_arrayref()->[0];
	$sth->finish();

	return $id;
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
    push @{ $self->{tx} }, [];
}

sub tx_commit
  {
    # public - commit current transaction
    
    my $self = shift;
    
    carp $error_no_transaction unless @{ $self->{tx} };
    
    # update lastObjectId's
    
    if (my $alloc_id = $self->{alloc_id})
      {
	my $table = $self->{schema}{class_table};
	
	for my $class_id (keys %$alloc_id)
	  {
	    my $id = $alloc_id->{$class_id};
	    next if $id < 0;
	    $self->sql_do("UPDATE $table SET lastObjectId = $id WHERE classId = $class_id");
	  }
	
	delete $self->{alloc_id};
      }
    
    unless ($self->{no_tx} || @{ $self->{tx} } > 1)
      {
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
			   local %done = ();
			   local $self->{defered} = [];
			   my $id = $self->_insert($_);
			   $self->do_defered;
			   $id;
		   } @objs;
	   }, $self, @objs );

    return wantarray ? @ids : shift @ids;
}

sub _insert
{
    my ($self, $obj) = @_;
    my $schema = $self->{schema};

    return $self->id($obj)
      if $self->id($obj);

    $done{$obj} = 1;

    my $class = ref $obj;
    my $classId = $self->{class2id}{$class} or unknown_classid $class;

    my $id = $self->make_id($classId);

    $self->welcome($obj, $id);
    $self->tx_on_rollback( sub { $self->goodbye($obj, $id) } );

	my $dbh = $self->{db};
	my $cache = $self->get_export_cache($class);

	my $context = { storage => $self, dbh => $dbh, id => $id };

	my @fields = $cache->{exporter}->($obj, $context);
	# print "*** @fields\n";

	for my $insert (@{ $cache->{insert} })
	  {
		my ($sth, $src, $cols, $root) = @$insert;

		$sth = $insert->[0] = $self->prepare($src)
		  unless $sth;

		my @meta = ($id);
		push @meta, $classId if $root;

		print $Tangram::TRACE "executing $src with (",
		join(', ', @meta, map { $_ || 'NULL' } @fields[0..$cols-1]), ")\n"
		  if $Tangram::TRACE;

		$sth->execute(@meta, splice(@fields, 0, $cols));

		$sth->finish();
	  }

    return $id;
}

sub auto_insert
  {
    # private - convenience sub for Refs, will be moved there someday
	
    my ($self, $obj, $table, $col, $id, $deep_update) = @_;
	
    return undef unless $obj;
	
    if (exists $done{$obj})
	  {
		# object is being saved already: we have a cycle
		
		$self->defer( sub
					  {
						# now that the object has been saved, we have an id for it
						my $obj_id = $self->id($obj);
						
						# patch the column in the referant
						$self->sql_do( "UPDATE $table SET $col = $obj_id WHERE id = $id" );
					  } );
		
		return undef;
	  }
	
    $self->_save($obj) if $deep_update;
	
    return $self->id($obj)		# already persistent
      || $self->_insert($obj);	# autosave
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
			 local %done = ();
			 local $self->{defered} = [];

			 $self->_update($obj);
			 $self->do_defered;
		     }
		   }, $self, @objs);
}

sub get_export_cache
  {
    my ($self, $class) = @_;

	my $cache = $self->{cache}{$class} ||= {};
    my $schema = $self->{schema};
    my $types = $schema->{types};

	my $context = { schema => $schema };
	
	unless ($cache->{exporter})
	  {
		my (@export_sources, @export_closures);

		$schema->visit_up($class,
						  sub
						  {
							my $class = shift;
							my $classdef = $schema->classdef($class);
							my $fields = $classdef->{fields};
							my @cols;
							
							$context->{class} = $class;

							foreach my $typetag (keys %$fields) {
							  push @cols, $types->{$typetag}->get_export_cols($fields->{$typetag});

							  for my $exporter ($types->{$typetag}->get_exporters($fields->{$typetag}, $context)) {
								if (ref $exporter) {
								  push @export_closures, $exporter;
								  push @export_sources, 'shift(@closures)->($obj, $context)';
								} else {
								  push @export_sources, $exporter;
								}
							  }
							}
							
							if (@cols) {
							  my $assigns = join ', ', map { "$_ = ?" } @cols;
							  my $update = "UPDATE $classdef->{table} SET $assigns WHERE id = ?";
							  #print "$update\n";
							  push @{ $cache->{update} }, [ undef, $update, scalar(@cols) ];
							}

							my $root = !@{ $classdef->{bases} };

							my @metacols = 'id';

							if ($root)
							  {
								push @metacols, 'classId';
							  }

							if (@cols || $root) {
							  my $cols = join ', ', @metacols, @cols;
							  my $vals = join ', ', map { '?' } @metacols, @cols;
							  my $insert = "INSERT INTO $classdef->{table} ($cols) VALUES ($vals)";
							  # print "$insert\n";
							  push @{ $cache->{insert} }, [ undef, $insert, scalar(@cols), $root ];
							}

						  } );

		my $export_source = join ",\n", @export_sources;

		my $copy_closures = @export_closures ? ' my @closures = @export_closures;' : '';

		$export_source = "sub { my (\$obj, \$context) = \@_;$copy_closures\n$export_source }";

		print $Tangram::TRACE "Compiling exporter for $class...\n$export_source\n"
		  if $Tangram::TRACE;

		# use Data::Dumper; print Dumper \@cols;
		$cache->{exporter} = eval $export_source or die;
	  };

	return $cache;
  }

sub _update
  {
    my ($self, $obj) = @_;

    my $id = $self->id($obj) or confess "$obj must be persistent";

    $done{$obj} = 1;

    my $class = ref $obj;
	my $dbh = $self->{db};
	my $cache = $self->get_export_cache($class);

	my $context = { storage => $self, dbh => $dbh, id => $id };

	my @fields = $cache->{exporter}->($obj, $context);
	#print "@fields\n";

	for my $update (@{ $cache->{update} })
	  {
		my ($sth, $src, $cols) = @$update;
		$sth = $update->[0] = $self->prepare($src)
		  unless $sth;
		$sth->execute(splice(@fields, 0, $cols), $id);
		$sth->finish();
	  }
  }

#############################################################################
# save

sub save
{
    my $self = shift;

    foreach my $obj (@_)
    {
	if ($self->id($obj))
	{
	    $self->update($obj)
	}
	else
	{
	    $self->insert($obj)
	}
    }
}

sub _save
{
    my $self = shift;
    foreach my $obj (@_)
    {
        if ($self->id($obj))
	{
	    $self->_update($obj)
	}
	else
	{
	    $self->_insert($obj)
	}
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

		   foreach my $obj (@objs) # causes memory leak??
		     {
		       my $id = $self->id($obj) or confess "object $obj is not persistent";

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

		       $schema->visit_down(ref($obj),
					   sub
					   {
					     my $class = shift;
					     my $classdef = $classes->{$class};
					     $self->sql_do("DELETE FROM $classdef->{table} WHERE id = $id")
					       unless $classdef->{stateless};
					   } );

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

    my $class = $self->{id2class}{ int(substr($id, -$self->{cid_size})) };

	my ($row, $alias) = _fetch_object_state($self, $id, $class);

    my $obj = $self->read_object($id, $class, $row, $alias->parts);

    # ??? $self->{-residue} = \@row;

    return $obj;
}

sub reload
{
    my $self = shift;

    return map { scalar $self->load( $_ ) } @_ if wantarray;

	my $obj = shift;
    my $id = $self->id($obj) or die "'$obj' is not persistent";
    my $class = $self->{id2class}{ int(substr($id, -$self->{cid_size})) };

	my ($row, $alias) = _fetch_object_state($self, $id, $class);
    _row_to_object($self, $obj, $id, $class, $row, $alias->parts);

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
    my ($self, $obj, $id, $class, $row, @parts) = @_;

    my $schema = $self->{schema};
	my $classes = $schema->{classes};
    my $types = $schema->{types};

	my $cache = $self->{transfer_cache}{$class} ||= do
	{
		my @cache;

		foreach my $class (@parts)
		{
			my $fields = $classes->{$class}{fields};

			for my $typetag (keys %$fields)
			{
				my $type = $types->{$typetag};

				push @cache,
					[
					 $type->can('read'), $type,
					 $fields->{$typetag}, $class,
					];
			}
		}

		$schema->visit_up($class,
		    sub
			{
				my $class = shift;
				my $classdef = $classes->{$class};

				if ($classdef->{stateless})
				{
					my $fields = $classdef->{fields};

					foreach my $typetag (keys %$fields)
					{
						my $type = $types->{$typetag};

						push @cache,
							[
							 $type->can('read'), $type,
							 $fields->{$typetag}, $class,
							];
					}
				}
			} );

		\@cache;
	};

	for my $transfer (@$cache)
	{
		my ($method, $type, $field, $class) = @$transfer;
		$method->($type, $row, $obj, $field, $self, $class);
	}

	return $obj;
}

sub _fetch_object_state
{
    my ($self, $id, $class) = @_;

    my $alias = Tangram::CursorObject->new($self, $class);
    my $select = $alias->cols;
    my $from = $alias->from;
    my $where = join ' AND ', $alias->where, " t" . $alias->root_table . ".id = $id";
    my $sql = "SELECT $select FROM $from WHERE $where";
   
    my $cursor = $self->sql_cursor($sql, $self->{db});
    my $row = [ $cursor->fetchrow() ];
    $cursor->close();

	die "no object with id '$id'" unless @$row;
   
    splice @$row, 0, 2; # id and classId

	return ($row, $alias);
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

sub DESTROY
{
    my ($self) = @_;
    carp "Tangram::Storage '$self' destroyed without explicit disconnect" if keys %$self;
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

package Tangram::Storage;

use DBI;
use Carp;

use base qw( Tangram::AbstractStorage );

use vars qw( %storage_class );

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

    new Tangram::Storage::Statement( statement => $sth, storage => $self,
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

1;
