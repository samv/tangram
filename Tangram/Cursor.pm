# (c) Sound Object Logic 2000-2001

use strict;

package Tangram::Cursor;

use vars qw( $stored %done );
use Carp;

sub new
{
	my ($pkg, $storage, $remote, $conn) = @_;

	confess unless $conn;

	$remote = $storage->remote($remote)
	  unless ref $remote;

	my $self = {};

	$self->{TARGET} = $remote;
	$self->{STORAGE} = $storage;
	$self->{SELECTS} = [];
	$self->{CONNECTION} = $conn;
	$self->{OWN_CONNECTION} = $conn != $storage->{db};

	bless $self, $pkg;
}

sub DESTROY
  {
	my $self = shift;
	$self->close();
  }

sub close
  {
	my $self = shift;

	if ($self->{SELECTS}) {
	  for my $select ( @{ $self->{SELECTS} } ) {
		my $sth = $select->[1] or next;
		$sth->finish() if $sth->{Active};
	  }
	}

	$self->{CONNECTION}->disconnect()
	  if $self->{OWN_CONNECTION};
  }

sub select
{
	my $self = shift;

	my %args;

	if (@_ > 1)
	{
		%args = @_;
	}
	else
	{
		$args{filter} = shift;
	}

	$self->{-order} = $args{order};
	$self->{-desc} = $args{desc};
	$self->{-distinct} = $args{distinct};
	$self->{-limit} = $args{limit};

	$self->retrieve( @{ $args{retrieve} } ) if exists $args{retrieve};

	my $target = $self->{TARGET};

	my (@filter_from, @filter_where);

	my $filter = Tangram::Filter->new( tight => 100, objects => Set::Object->new() );

	if (my $user_filter = $args{filter})
	{
		$filter->{expr} = $user_filter->{expr};
		$filter->{objects}->insert($user_filter->{objects}->members);
		$filter->{objects}->remove($target->object);
	}

	$self->{SELECTS} = [ map {
	  [ $self->build_select( $_, [], [ $filter->from ],  [ $filter->where ]), undef, $_ ]
	} $self->{STORAGE}->get_polymorphic_select( $target->class ) ];

	$self->{position} = -1;

	return $self->execute();
}

sub execute
  {
	my ($self) = @_;
	return $self->{-current} if $self->{position} == 0;
	$self->{cur_select} = [ @{ $self->{SELECTS} } ];
	return $self->prepare_next_statement() && $self->next();
  }

sub prepare_next_statement
  {
	my ($self) = @_;

	my $select = $self->{ACTIVE} = shift @{ $self->{cur_select} } or return undef;

	my ($sql, $sth, $template) = @$select;

	$self->{sth}->finish() if $self->{sth};

	$sth = $select->[1] = $self->{STORAGE}->sql_prepare($sql, $self->{CONNECTION})
	  unless $sth;

	$self->{sth} = $sth;

	$sth->execute();

	return $sth;
  }

sub build_select
{
	my ($self, $template, $cols, $from, $where) = @_;

	if (my $retrieve = $self->{-retrieve})
	{
		@$cols = map { $_->{expr} } @$retrieve;
	}

	my $select = $template->instantiate( $self->{TARGET}, $cols, $from, $where);
	
	substr($select, length('SELECT'), 0) = ' DISTINCT'
	  if $self->{-distinct};

	if (my $order = $self->{-order})
	{
		$select .= "\n\tORDER BY " . join ', ', map { $_->{expr} } @$order;
	}

	if ($self->{-desc})
	{
		$select .= ' DESC';
	}

	$select .= " LIMIT $self->{-limit}" if defined $self->{-limit};

	return $select;
}

sub _next
{
	my ($self) = @_;

	$self->{-current} = undef;
	++$self->{position};

	my $sth = $self->{sth};
	my @row;

	while (1)
	{
		@row = $sth->fetchrow();
		last if @row;
		$sth = $self->prepare_next_statement() or return undef;
	}

	my $storage = $self->{STORAGE};

	my ($id, $classId, $state) = $self->{ACTIVE}[-1]->extract(\@row);

	$id = $storage->{import_id}->($id, $classId);

	my $class = $storage->{id2class}{$classId} or die "unknown class id $classId";

	# even if object is already loaded we must read it so that @rpw only contains residue
	my $obj = $storage->read_object($id, $class, $state);

	$self->{-residue} = exists $self->{-retrieve}
		? [ map { ref $_ ? $_->{type}->read_data(\@row) : shift @row } @{$self->{-retrieve}} ]
			: \@row;

	# if object is already loaded return previous copy
	$obj = $storage->{objects}{$id} if exists $storage->{objects}{$id};
   
	$self->{-current} = $obj;

	return $obj;
}

sub next
{
	my ($self) = @_;

	return $self->_next unless wantarray;

	my ($obj, @results);

	while (defined($obj = $self->_next))
	{
		push @results, $obj;
	}

	return @results;
}

sub current
{
	my ($self) = @_;
	$self->{-current}
}

sub retrieve
{
	my $self = shift;
	push @{$self->{-retrieve}}, @_;
}

sub residue
{
	@{shift->{-residue}};
}

sub object
{
	my ($self) = @_;
	return $self->{object};
}

package Tangram::DataCursor;

use Carp;

sub open
{
	my ($type, $storage, $select, $conn) = @_;
   
	confess unless $conn;
	
	bless
	{
	 select => $select,
	 cursor => $storage->sql_cursor(substr($select->{expr}, 1, -1), $conn),
	}, $type;
}

sub fetchrow
{
	my $self = shift;
	my @row = $self->{cursor}->fetchrow;
	return () unless @row;
	map { $_->{type}->read_data(\@row) } @{$self->{select}{cols}};
}

sub fetchall_arrayref
{
	my $self = shift;
	my @results;

	while (my @row = $self->fetchrow)
	{
		push @results, [ @row ];
	}

	return \@results;
}

sub new
{
	my $pkg = shift;
	return bless [ @_ ] , $pkg;
}

1;

__END__

sub _select
{
	my ($self, $class, $filter) = @_;

	return if exists $done{$class};

	$done{$class} = 1;

	my $storage = $self->{STORAGE};
	my $schema = $storage->{schema};
	my $classes = $schema->{classes};
	my $classdef = $classes->class($class);
	my $class2id = $storage->{class2id};
	my $stored = $self->{-stored};

	if ($classdef->{specs} && @{$classdef->{specs}} || $classdef->{stateless})
	{
		my @shared = ( ($classdef->{abstract} ? () : $class), # concat lists
					   map { _select_shared($_, $classes) } @{$classdef->{specs}} );

		if (@shared)
		{
			my $cols = $stored->cols;
			my $from = $filter->from;
			my $cid = $stored->class_id_col;

			my $where = join ' AND ',
				"$cid IN (" . join(', ', map { $storage->class_id($_) } @shared) . ')',
					$filter->where;
         
			push @{$self->{SELECTS}}, [ $self->build_select($cols, $from, $where), undef, $stored->parts ];
		}

		foreach my $spec (@{$classdef->{specs}})
		{
			$self->_select_unshared($spec, $filter);
		}
	}

	elsif (!$classdef->{abstract})
	{
		my $cols = $stored->cols;
		my $from = $filter->from;
		my $where = $filter->where;
		push @{$self->{SELECTS}}, [ $self->build_select($cols, $from, $where), undef, $stored->parts ];
	}
}

sub _select_shared
{
	my ($class, $classes) = @_;

	return () if $done{$class};

	my $classdef = $classes->class($class);
	return () unless $classdef->{stateless} && @{ $classdef->{bases} } <= 1;

	$done{$class} = 1;

	( ($classdef->{abstract} ? () : $class), # concat lists
	  map { _select_shared($_, $classes) } @{$classdef->{specs}} );
}

sub _select_unshared
{
	my ($self, $class, $filter) = @_;

	my $classes = $self->{STORAGE}{schema}{classes};
	my $classdef = $classes->{$class};

	if ($classdef->{stateless} && @{ $classdef->{bases} } <= 1)
	{
		foreach my $spec (@{$classdef->{specs}})
		{
			$self->_select_unshared($spec, $filter);
		}
	}
	else
	{
		my $mark = $stored->mark();
      
		$stored->push_spec($class) unless $classdef->{stateless};

		my $bases = $classdef->{bases};

		if (@$bases > 1)
		{
			my $schema = $self->{STORAGE}{schema};

			foreach my $base ( @$bases )
			{
				next if $done{$base};

				$schema->visit_up( $base,
								   sub
								   {
									   my $base = shift;
									   $stored->push_spec( $base ) unless $classes->{$base}{stateless} || $done{$base};
									   $done{$base} = 1;
								   } );
			}
		}

		$self->_select($class, $filter);
      
		$stored->pop_spec($mark);
	}
}


sub _obsolete_build_select
{
	my ($self, $cols, $from, $where) = @_;

	if (my $retrieve = $self->{-retrieve})
	{
		$cols = join ', ', $cols, map { $_->{expr} } @$retrieve;
	}

	my $select = "SELECT";

	$select .= ' DISTINCT' if $self->{-distinct};

	$select .= " $cols\n\tFROM $from\n\t";
	$select .= "WHERE $where" if $where;

	if (my $order = $self->{-order})
	{
		$select .= "\n\tORDER BY " . join ', ', map { $_->{expr} } @$order;
	}

	if ($self->{-desc})
	{
		$select .= ' DESC';
	}

	$select .= " LIMIT $self->{-limit}" if defined $self->{-limit};

	return $select;
}
