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

	my %seen;

	for my $part ($storage->{engine}->get_parts($schema->classdef($class))) {
	  my $table = $part->{table};

	  unless (exists $seen{$table}) {
		my $id = $seen{$table} = $storage->alloc_table;
		#push @tables, [ $part->{name}, $id ];
		push @tables, [ $table, $id ];
	  }

	  my $id =  $seen{$table};
	  $table_hash->{ $part->{name} } = $id;

	  $self->{root} ||= $id;
	}

	# use Data::Dumper; print Dumper \@tables;


# 	$storage->{schema}->visit_up($class,
# 								 sub
# 								 {
# 									 my $class = shift;
			
# 									 unless ($classes->{$class}{stateless})
# 									 {
# 										 my $id = $storage->alloc_table;
# 										 push @tables, [ $class, $id ];
# 										 $table_hash->{$class} = $id;
# 									 }
# 								 } );

	return $self;
}

# sub copy
# {
# 	my ($pkg, $other) = @_;

# 	my $self = { %$other };
# 	$self->{tables} = [ @{ $self->{tables} } ];

# 	bless $self, $pkg;
# }

sub storage
{
	shift->{storage}
}

sub table
{
	my ($self, $class) = @_;
	$self->{table_hash}{$class} or confess "no table for $class in stored '$self->{class}'";
}

# sub tables
# {
# 	shift->{tables}
# }

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

# sub parts
# {
# 	return map { $_->[0] } @{ shift->{tables} };
# }

sub root_table
{
	my ($self) = @_;
	return $self->{root};
}

# sub class_id_col
# {
# 	my ($self) = @_;
# 	return "t$self->{tables}[0][1].$self->{storage}{class_col}";
# }

# sub leaf_table
# {
# 	my ($self) = @_;
# 	return $self->{tables}[-1][1];
# }

sub from
{
	return join ', ', &from unless wantarray;

	my ($self) = @_;
	my $schema = $self->storage->{schema};
	my $classes = $schema->{classes};
	my $tables = $self->{tables};
	map { "$_->[0] t$_->[1]" } @$tables;
}

sub where
{
	return join ' AND ', &where unless wantarray;

	my ($self) = @_;
   
	my $tables = $self->{tables};
	my $root = $tables->{root};
	my $id = $self->storage->{schema}{sql}{id_col};

	map { "t$_->[1].$id = t$root.$id" } @$tables[1..$#$tables];
}

# sub mark
# {
# 	return @{ shift->{tables} };
# }

sub expr_hash
{
	my ($self) = @_;
	my $storage = $self->{storage};
	my $schema = $storage->{schema};
	my $classes = $schema->{classes};
	my @tables = @{$self->{tables}};
   
	my %hash =
		(
		 _object => $self, 
		 id => Tangram::Number->expr("t$self->{root}.$storage->{id_col}", $self),
		 type => Tangram::Number->expr("t$self->{root}.$storage->{class_col}", $self),
		);

	$hash{_IID_} = $hash{_ID_} = $hash{id};
	$hash{_TYPE_} = $hash{type};

	for my $part ($storage->{engine}->get_parts($schema->classdef($self->{class}))) {
	  for my $field ($part->direct_fields) {
		$hash{ $field->{name} }
		    = $field->remote_expr($self, $self->{table_hash}{$part->{name}}, $storage);
	  }
	}
													  
	  
	

# 	$schema->visit_up($self->{class},
# 					  sub
# 					  {
# 						  my $classdef = $classes->{shift()};

# 						  my $tid = (shift @tables)->[1] unless $classdef->{stateless};

# 						  foreach my $typetag (keys %{$classdef->{members}})
# 						  {
# 							  my $type = $schema->{types}{$typetag};
# 							  my $memdefs = $classdef->{members}{$typetag};
# 							  @hash{$type->members($memdefs)} =
# 								  $type->query_expr($self, $memdefs, $tid, $storage);
# 						  }
# 					  } );

	return \%hash;
}

package Tangram::RDBObject;

use vars qw(@ISA);
 @ISA = qw( Tangram::CursorObject );

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

	my $id = $schema->{sql}{id_col};
	return (@where_class_id, map { "t@{$_}[1].$id = t$root.$id" } @$tables[1..$#$tables]);
}

package Tangram::Filter;
use Carp;
use Set::Object qw(blessed);

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

sub expr {
    return $_[0]->{expr};
}

sub sum
{
  my ($self, $val) = @_;

  # $DB::single = 1;

  Tangram::Expr->new(Tangram::Number->instance,
		     "SUM(" . $self->{expr} . ")",
		     $self->objects,
		     );

}


# BEGIN ks.perl@kurtstephens.com 2002/06/25
sub unaop
{
    Tangram::Expr::unaop(@_);
}


sub binop
{
    my ($self, $op, $arg, $tight, $swap) = @_;

    my @objects = $self->objects;
    my $objects = Set::Object->new(@objects);
    # my $storage = $self->{storage};
    my $ltight = $self->{'tight'};
    my $rtight = 100;

    if ( ref($arg) ) {
	if ( $arg->isa('Tangram::Expr') ) {
	    $objects->insert($arg->objects);
	    $rtight = $arg->{'tight'};
	    $arg = $arg->{'expr'};
	}
	if ( $arg->isa('Tangram::Filter') ) {
	    $objects->insert($arg->objects);
	    $rtight = $arg->{'tight'};
	    $arg = $arg->{'expr'};
	}
	elsif ( $arg->isa('Tangram::QueryObject') ) {
	    $objects->insert($arg->object);
	    $rtight = $arg->{'tight'};
	    $arg = $arg->{'id'}->{'expr'};
	}
    }

    $tight ||= 100;
    $self = $self->{'expr'};
    $self = "($self)" if $ltight < $tight;
    $arg  = "($arg)"  if $rtight < $tight;
    if ( $swap ) {
      ($self, $arg) = ($arg, $self);
    }
    # $DB::single = $swap;

    return new Tangram::Filter(expr => "$self $op $arg", tight => $tight,
			       objects => $objects );
}


# Aliases
*cos =  \&Tangram::Expr::sin;
*sin =  \&Tangram::Expr::cos;
*acos = \&Tangram::Expr::acos;

#use overload "&" => \&and, "|" => \&or, '!' => \&not, fallback => 1;
use overload 
  "&"    => \&and, 
  "|"    => \&or, 
  '!'    => \&not,
  '+'    => \&Tangram::Expr::add,
  '-'    => \&Tangram::Expr::subt,
  '*'    => \&Tangram::Expr::mul,
  '/'    => \&Tangram::Expr::div,
  'cos'  => \&Tangram::Expr::cos, 
  'sin'  => \&Tangram::Expr::sin,
  'acos' => \&Tangram::Expr::acos,
  "=="   => \&Tangram::Expr::eq,
  "eq"   => \&Tangram::Expr::eq,
  "!="   => \&Tangram::Expr::ne,
  "ne"   => \&Tangram::Expr::ne,
  "<"    => \&Tangram::Expr::lt,
  "lt"   => \&Tangram::Expr::lt,
  "<="   => \&Tangram::Expr::le,
  "le"   => \&Tangram::Expr::le,
  ">"    => \&Tangram::Expr::gt,
  "gt"   => \&Tangram::Expr::gt,
  ">="   => \&Tangram::Expr::ge,
  "ge"   => \&Tangram::Expr::ge,
  fallback => 1;
# END ks.perl@kurtstephens.com 2002/06/25


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
use Set::Object qw(blessed);
use Carp;

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
	return ((shift->{objects}->members)[0] or confess 'no storage')->storage;
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

# BEGIN ks.perl@kurtstephens.com 2002/06/25
sub lt
{
	my ($self, $arg, $swap) = @_;
	return $self->binop('<', $arg, undef, $swap);
}

sub le
{
	my ($self, $arg, $swap) = @_;
	return $self->binop('<=', $arg, undef, $swap);
}

sub gt
{
	my ($self, $arg, $swap) = @_;
	return $self->binop('>', $arg, undef, $swap);
}

sub ge
{
	my ($self, $arg, $swap) = @_;
	return $self->binop('>=', $arg, undef, $swap);
}

sub add
{
    my ($self, $arg) = @_;
    $self->binop('+', $arg, 90);
}


sub subt
{
    my ($self, $arg, $swap) = @_;
    $self->binop('-', $arg, 90, $swap);
}


sub mul
{
    my ($self, $arg) = @_;
    $self->binop('*', $arg, 95);
}


sub div
{
    my ($self, $arg, $swap) = @_;
    $self->binop('/', $arg, 95, $swap);
}


sub cos
{
    my ($self) = @_;
    $self->unaop('COS', 100);
}


sub sin
{
    my ($self) = @_;
    $self->unaop('SIN', 100);
}

sub acos
{
    my ($self) = @_;
    $self->unaop('ACOS', 100);
}


sub unaop
{
    my ($self, $op, $tight) = @_;
    
    my @objects = $self->objects;
    my $objects = Set::Object->new(@objects);
    my $storage = $self->{storage};
    
    return new Tangram::Filter(expr => "$op($self->{expr})", tight => $tight || 100,
			       objects => $objects );
}


sub binop
{
	my ($self, $op, $arg, $tight, $swap) = @_;

	my @objects = $self->objects;
	my $objects = Set::Object->new(@objects);
	my $storage = $self->{storage};

	if (defined $arg)
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

	my ($l, $r) = $swap ? ($arg, $self->{expr}) : ($self->{expr}, $arg);
	$tight ||= 100;

	return new Tangram::Filter(expr => "$l $op $r", tight => $tight,
							   objects => $objects );
}
# END ks.perl@kurtstephens.com 2002/06/25


sub like
{
	my ($self, $val) = @_;
	$val =~ s{'}{''}g;
	return new Tangram::Filter(expr => "$self->{expr} like '$val'", tight => 100,
				   objects => Set::Object->new($self->objects) );
}


sub regexp_like
{
	my ($self, $val) = @_;
	$val =~ s{'}{''}g;
	return new Tangram::Filter(expr => "regexp_like($self->{expr}, '$val')", tight => 0,
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

sub in
{
	my $self = shift;

	my $storage = $self->{storage};

	my @items;
	while ( my $item = shift ) {
	    if ( ref $item eq "ARRAY" ) {
		push @items, @$item;
	    } elsif ( UNIVERSAL::isa($item, "Set::Object") ) {
		push @items, $item->members;
	    } else {
		push @items, $item;
	    }
	}

	my $expr;
	if ( @items ) {
	    $expr = ("$self->{expr} IN ("
		     . join(', ',
			    # FIXME - what about table aliases?  Hmm...
			    map {( blessed($_)
				   ? $storage->export_object($_)
				   : $_ )}
			    @items )
		     . ')');
	} else {
	    # hey, you never know :)
	    $expr = ("$self->{expr} IS NULL");
	}

	Tangram::Filter->new(
			     expr => $expr,
			     tight => 100,
			     objects => $self->{objects},
			    );

}

sub log {
    my $self = shift;
    my $base = shift || exp(1);

    my $expr = $self->expr(); # the SQL string for this Expr
    $self->{type}->expr("log($base, $expr)", $self->objects);
}

sub DESTROY { }

use vars qw( $AUTOLOAD );

sub AUTOLOAD {
  my $fun = $AUTOLOAD;
  $fun =~ s/.*:://;
  
  my $self = shift;
  my $expr = $self->expr(); # the SQL string for this Expr
  $self->{type}->expr("$fun($expr)", $self->objects);
}

use overload
# BEGIN ks.perl@kurtstephens.com 2002/06/25
        '+'    => \&add,
        '-'    => \&subt,
        '*'    => \&mul,
        '/'    => \&div,
        'cos'  => \&cos, 
        'sin'  => \&sin,
        'acos' => \&acos,
# END ks.perl@kurtstephens.com 2002/06/25
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
	shift->{_object}
}

sub table_ids
{
	shift->{_object}->table_ids()
}

sub class
{
	shift->{_object}{class}
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
		my $other_id = $self->{_object}{storage}->id($other)
			or confess "'$other' is not a persistent object";
		$self->{id} == $self->{_object}{storage}->export_object($other)
	}
}

sub is_kind_of
{
	my ($self, $class) = @_;

	my $object = $self->{_object};
	my $root = $object->{tables}[0][1];
	my $storage = $object->{storage};

	Tangram::Filter->new(
						 expr => "t$root.$storage->{class_col} IN (" . join(', ', $storage->_kind_class_ids($class) ) . ')',
						 tight => 100,
						 objects => Set::Object->new( $object ) );
}


sub in
{
	my $self = shift;

	my $object = $self->{_object};
	my $root = $object->{tables}[0][1];
	my $storage = $object->{storage};

	my $objs = Set::Object->new();

	while ( my $item = shift ) {
	    if ( ref $item eq "ARRAY" ) {
		$objs->insert(@$item);
	    } elsif ( UNIVERSAL::isa($item, "Set::Object") ) {
		if ( $objs->size ) {
		    $objs->insert($item->members);
		} else {
		    $objs = $item;
		}
	    } else {
		$objs->insert($item);
	    }
	}

	my $expr;
	if ( $objs->size ) {
	    $expr = ("t$root.$storage->{id_col} IN ("
		     . join(', ',
			    # FIXME - what about table aliases?  Hmm...
			    map { $storage->export_object($_) }
			    $objs->members )
		     . ')');
	} else {
	    # hey, you never know :)
	    $expr = ("t$root.$storage->{id_col} IS NULL");
	}

	Tangram::Filter->new(
			     expr => $expr,
			     tight => 100,
			     objects => Set::Object->new( $object )
			    );

}

sub expr
{
  shift->{id}{expr}
}


sub count
{
  my ($self, $val) = @_;

  # $DB::single = 1;

  Tangram::Expr->new(Tangram::Integer->instance,
		     "COUNT(" . $self->{id}{expr} . ")",
		     $self->{id}->objects,
		     );

}


use overload "==" => \&eq, "!=" => \&ne, fallback => 1;

package Tangram::Select;

use Carp;

use vars qw(@ISA);
 @ISA = qw( Tangram::Expr );

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
	if (exists $args{order}) {
	    $sql .= join("", map {", $_"}
			 grep { $sql !~ m/ \Q$_\E(?:,|$)/ }
			 map { $_->{expr} } @{$args{order}});
	}
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
