# (c) Sound Object Logic 2000-2001

use strict;

package Tangram::Table;

sub new
{
	my ($pkg, $name, $alias) = @_;
	bless [ $name, $alias ], $pkg;
}

sub from
{
	return "@{shift()}";
}

sub where
{
	()
}

package Tangram::CursorObject;
use Carp;

sub new
{
	my ($pkg, $storage, $class) = @_;

	my $schema = $storage->{schema};
	my $classes = $schema->{classes};
	$schema->check_class($class);

	my @tables;
	my $table_hash = { };
	my $self = bless { storage => $storage, tables => \@tables, class => $class,
					   table_hash => $table_hash }, $pkg;

	$storage->{schema}->visit_up($class,
								 sub
								 {
									 my $class = shift;
			
									 unless ($classes->{$class}{stateless})
									 {
										 my $id = $storage->alloc_table;
										 push @tables, [ $class, $id ];
										 $table_hash->{$class} = $id;
									 }
								 } );

	return $self;
}

sub copy
{
	my ($pkg, $other) = @_;

	my $self = { %$other };
	$self->{tables} = [ @{ $self->{tables} } ];

	bless $self, $pkg;
}

sub storage
{
	shift->{storage}
}

sub table
{
	my ($self, $class) = @_;
	$self->{table_hash}{$class} or confess "no table for $class in stored '$self->{class}'";
}

sub tables
{
	shift->{tables}
}

sub class
{
	shift->{class}
		#my ($self) = @_;
		#my $tables = $self->{tables};
		#return $tables->[$#$tables][0];
}

sub table_ids
{
	return map { $_->[1] } @{ shift->{tables} };
}

sub parts
{
	return map { $_->[0] } @{ shift->{tables} };
}

sub root_table
{
	my ($self) = @_;
	return $self->{tables}[0][1];
}

sub class_id_col
{
	my ($self) = @_;
	return "t$self->{tables}[0][1].$self->{storage}{class_col}";
}

sub leaf_table
{
	my ($self) = @_;
	return $self->{tables}[-1][1];
}

sub from
{
	return join ', ', &from unless wantarray;

	my ($self) = @_;
	my $schema = $self->storage->{schema};
	my $classes = $schema->{classes};
	my $tables = $self->{tables};
	map { "$classes->{$_->[0]}{table} t$_->[1]" } @$tables;
}

sub where
{
	return join ' AND ', &where unless wantarray;

	my ($self) = @_;
   
	my $tables = $self->{tables};
	my $root = $tables->[0][1];

	map { "t@{$_}[1].id = t$root.id" } @$tables[1..$#$tables];
}

sub mark
{
	return @{ shift->{tables} };
}

sub expr_hash
{
	my ($self) = @_;
	my $storage = $self->{storage};
	my $schema = $storage->{schema};
	my $classes = $schema->{classes};
	my @tables = @{$self->{tables}};
	my $root_tid = $tables[0][1];
   
	my %hash =
		(
		 object => $self, 
		 id => Tangram::Number->expr("t$root_tid.id", $self)
		);

	$schema->visit_up($self->{class},
					  sub
					  {
						  my $classdef = $classes->{shift()};

						  my $tid = (shift @tables)->[1] unless $classdef->{stateless};

						  foreach my $typetag (keys %{$classdef->{members}})
						  {
							  my $type = $schema->{types}{$typetag};
							  my $memdefs = $classdef->{members}{$typetag};
							  @hash{$type->members($memdefs)} =
								  $type->query_expr($self, $memdefs, $tid, $storage);
						  }
					  } );

	return \%hash;
}

package Tangram::RDBObject;

use base qw( Tangram::CursorObject );

sub where
{
	return join ' AND ', &where unless wantarray;

	my ($self) = @_;
   
	my $storage = $self->{storage};
	my $schema = $storage->{schema};
	my $classes = $schema->{classes};
	my $tables = $self->{tables};
	my $root = $tables->[0][1];
	my $class = $self->{class};

	my @where_class_id;

	if ($classes->{$class}{stateless})
	{
		my @class_ids;

		push @class_ids, $storage->class_id($class) unless $classes->{$class}{abstract};

		$schema->for_each_spec($class,
							   sub { my $spec = shift; push @class_ids, $storage->class_id($spec) unless $classes->{$spec}{abstract} } );

		@where_class_id = "t$root.$storage->{class_col} IN (" . join(', ', $storage->_kind_class_ids($class) ) . ')';
	}

	return (@where_class_id, map { "t@{$_}[1].id = t$root.id" } @$tables[1..$#$tables]);
}

package Tangram::Filter;
use Carp;

sub new
{
	my $pkg = shift;
	my $self = bless { @_ }, $pkg;
	$self->{objects} ||= Set::Object->new;
	$self;
}

sub and
{
	my ($self, $other) = @_;
	return op($self, 'AND', 10, $other);
}

sub and_perhaps
{
	my ($self, $other) = @_;
	return $other ? op($self, 'AND', 10, $other) : $self;
}

sub or
{
	my ($self, $other) = @_;
	return op($self, 'OR', 9, $other);
}

sub not
{
	my ($self) = @_;

	Tangram::Filter->new(
						 expr => "NOT ($self->{expr})",
						 tight => 100,
						 objects => Set::Object->new(
													 $self->{objects}->members ) );
}

sub as_string
{
	my $self = shift;
	return ref($self) . "($self->{expr})";
}

use overload "&" => \&and, "|" => \&or, '!' => \&not, fallback => 1;

sub op
{
	my ($left, $op, $tight, $right) = @_;

	confess "undefined operand(s) for $op" unless $left && $right;

	my $lexpr = $tight > $left->{tight} ? "($left->{expr})" : $left->{expr};
	my $rexpr = $tight > $right->{tight} ? "($right->{expr})" : $right->{expr};

	return Tangram::Filter->new(
								expr => "$lexpr $op $rexpr",
								tight => $tight,
								objects => Set::Object->new(
															$left->{objects}->members, $right->{objects}->members ) );
}

sub from
{
	return join ', ', &from unless wantarray;
	map { $_->from } shift->objects;
}

sub where
{
	return join ' AND ', &where unless wantarray;

	my ($self) = @_;
	my @expr = "($self->{expr})" if exists $self->{expr};
	(@expr, map { $_->where } $self->objects);
}

sub where_objects
{
	return join ' AND ', &where_objects unless wantarray;
	my ($self, $object) = @_;
	map { $_ == $object ? () : $_->where } $self->objects;
}

sub objects
{
	shift->{objects}->members;
}

package Tangram::Expr;

sub new
{
	my ($pkg, $type, $expr, @objects) = @_;
	return bless { expr => $expr, type => $type,
				   objects => Set::Object->new(@objects),
				   storage => $objects[0]->{storage} }, $pkg;
}

sub expr
{
	return shift->{expr};
}

sub storage
{
	return shift->{objects}[0]->{storage};
}

sub type
{
	return shift->{type};
}

sub objects
{
	return shift->{objects}->members;
}

sub eq
{
	my ($self, $arg) = @_;
	return $self->binop('=', $arg);
}

sub ne
{
	my ($self, $arg) = @_;
	return $self->binop('<>', $arg);
}

sub lt
{
	my ($self, $arg) = @_;
	return $self->binop('<', $arg);
}

sub le
{
	my ($self, $arg) = @_;
	return $self->binop('<=', $arg);
}

sub gt
{
	my ($self, $arg) = @_;
	return $self->binop('>', $arg);
}

sub ge
{
	my ($self, $arg) = @_;
	return $self->binop('>=', $arg);
}

sub binop
{
	my ($self, $op, $arg) = @_;

	my @objects = $self->objects;
	my $objects = Set::Object->new(@objects);
	my $storage = $self->{storage};

	if ($arg)
	{
		if (my $type = ref($arg))
		{
			if ($arg->isa('Tangram::Expr'))
			{
				$objects->insert($arg->objects);
				$arg = $arg->{expr};
			}
   
			elsif ($arg->isa('Tangram::QueryObject'))
			{
				$objects->insert($arg->object);
				$arg = $arg->{id}->{expr};
			}
   
			elsif (exists $storage->{schema}{classes}{$type})
			{
				$arg = $storage->export_object($arg) or Carp::confess "$arg is not persistent";
			}

			else
			{
			    $arg = $self->{type}->literal($arg, $storage);
			}
		}
		else
		{
			$arg = $self->{type}->literal($arg, $storage);
		}
	}
	else
	{
		$op = $op eq '=' ? 'IS' : $op eq '<>' ? 'IS NOT' : Carp::confess;
		$arg = 'NULL';
	}

	return new Tangram::Filter(expr => "$self->{expr} $op $arg", tight => 100,
							   objects => $objects );
}

sub like
{
	my ($self, $val) = @_;
	return new Tangram::Filter(expr => "$self->{expr} like '$val'", tight => 100,
							   objects => Set::Object->new($self->objects) );
}

sub count
{
	my ($self, $val) = @_;
	$self->{storage}
		->expr(Tangram::Integer->instance, "COUNT($self->{expr})",
				$self->objects );
}

sub as_string
{
	my $self = shift;
	return ref($self) . "($self->{expr})";
}

use overload
	"==" => \&eq,
	"eq" => \&eq,
	"!=" => \&ne,
	"ne" => \&ne,
	"<" => \&lt,
	"lt" => \&lt,
	"<=" => \&le,
	"le" => \&le,
	">" => \&gt,
	"gt" => \&gt,
	">=" => \&ge,
	"ge" => \&ge,
	'""' => \&as_string,
	fallback => 1;

package Tangram::QueryObject;

use Carp;

sub new
{
	my ($pkg, $obj) = @_;
	bless $obj->expr_hash(), $pkg;
}

sub object
{
	shift->{object}
}

sub table_ids
{
	shift->{object}->table_ids()
}

sub class
{
	shift->{object}{class}
}

sub eq
{
	my ($self, $other) = @_;

	if (!defined($other))
	{
		$self->{id} == undef
	}
	elsif ($other->isa('Tangram::QueryObject'))
	{
		$self->{id} == $other->{id}
	}
	else
	{
		my $other_id = $self->{object}{storage}->id($other)
			or confess "'$other' is not a persistent object";
		$self->{id} == $self->{object}{storage}->export_object($other)
	}
}

sub is_kind_of
{
	my ($self, $class) = @_;

	my $object = $self->{object};
	my $root = $object->{tables}[0][1];
	my $storage = $object->{storage};

	Tangram::Filter->new(
						 expr => "t$root.$storage->{class_col} IN (" . join(', ', $storage->_kind_class_ids($class) ) . ')',
						 tight => 100,
						 objects => Set::Object->new( $object ) );
}

use overload "==" => \&eq, "!=" => \&ne, fallback => 1;

package Tangram::Select;

use Carp;

use base qw( Tangram::Expr );

sub new
{
	my ($type, %args) = @_;

	my $cols = join ', ', map
	{
		confess "column specification must be a Tangram::Expr" unless $_->isa('Tangram::Expr');
		$_->{expr};
	} @{$args{cols}};

	my $filter = $args{filter} || $args{where} || Tangram::Filter->new;

	my $objects = Set::Object->new();

	if (exists $args{from})
	{
		$objects->insert( map { $_->object } @{ $args{from} } );
	}
	else
	{
		$objects->insert( $filter->objects(), map { $_->objects } @{ $args{cols} } );
		$objects->remove( @{ $args{exclude} } ) if exists $args{exclude};
	}

	my $from = join ', ', map { $_->from } $objects->members;

	my $where = join ' AND ',
		$filter->{expr} ? "($filter->{expr})" : (),
			map { $_->where } $objects->members;

	my $sql = "SELECT";
	$sql .= ' DISTINCT' if $args{distinct};
	$sql .= "  $cols";
	$sql .= "\nFROM $from" if $from;
	$sql .= "\nWHERE $where" if $where;

	if (exists $args{order})
	{
		$sql .= "\nORDER BY " . join ', ', map { $_->{expr} } @{$args{order}};
	}

	my $self = $type->SUPER::new(Tangram::Integer->instance, "($sql)");
	
	$self->{cols} = $args{cols};

	return $self;
}

sub from
{
	my ($self) = @_;
	my $from = $self->{from};
	return $from ? $from->members : $self->SUPER::from;
}

sub where
{
}

sub execute
{
	my ($self, $storage, $conn) = @_;
	return Tangram::DataCursor->open($storage, $self, $conn);
}

1;
