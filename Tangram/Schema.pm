# (c) Sound Object Logic 2000-2001

use strict;

package Tangram::ClassHash;

use Carp;

sub class
{
   my ($self, $class) = @_;
   $self->{$class} or croak "unknown class '$class'";
}

package Tangram::Class;

sub members
{
   my ($self, $type) = @_;
   return @{$self->{$type}};
}

package Tangram::Schema;

use Carp;

use vars qw( %TYPES );

%TYPES = 
(
   %TYPES,
   ref      => new Tangram::Ref,
);

sub new
{
    my $pkg = shift;

	my $self = ref $_[0] ? shift() : { @_ };
    bless $self, $pkg;

    $self->{make_object} ||= sub { shift()->new() };
    $self->{class_table} ||= 'OpalClass';

	$self->{control} ||= 'tangram';

	$self->{sql}{default_null} = 'NULL' unless exists $self->{sql}{default_null};
	$self->{sql}{id_col} ||= 'id';
	$self->{sql}{id} ||= 'INTEGER';
	$self->{sql}{class_col} ||= 'class';
	$self->{sql}{cid} ||= 'INTEGER';
	$self->{sql}{oid} ||= 'INTEGER';
	$self->{sql}{cid_size} ||= 4;

    my $types = $self->{types} ||= {};

    %$types = ( %TYPES, %$types );

	my @class_list = ref($self->{'classes'}) eq 'HASH' ? %{ $self->{'classes'} } : @{ $self->{'classes'} };
    my $class_hash = $self->{'classes'} = {};

    bless $class_hash, 'Tangram::ClassHash';

    my $autoid = 0;

    while (my ($class, $def) = splice @class_list, 0, 2)
    {
		my $classdef = $class_hash->{$class} = $def;

		if (exists $def->{id}) {
		  $autoid = $def->{id};
		} else {
		  $def->{id} = ++$autoid;
		}

		bless $classdef, 'Tangram::Class';

		$classdef->{table} ||= $class;

		$classdef->{fields} ||= $classdef->{members};
		$classdef->{members} = $classdef->{fields};

		my $cols = 0;

		foreach my $typetag (keys %{$classdef->{members}})
		{
			my $memdefs = $classdef->{members}{$typetag};
	    
			$memdefs = $classdef->{members}{$typetag} =
			{ map { $_, $_ } @$memdefs } if (ref $memdefs eq 'ARRAY');

			my $type = $self->{types}{$typetag};

			croak("Unknow field type '$typetag', ",
				  "did you forget some 'use Tangram::SomeType' ",
				  "in your program?\n")
				unless defined $types->{$typetag};

			my @members = $types->{$typetag}->reschema($memdefs, $class, $self)
				if $memdefs;

			@{$classdef->{member_type}}{@members} = ($type) x @members;
	    
			@{$classdef->{MEMDEFS}}{keys %$memdefs} = values %$memdefs;
	    
			local $^W = undef;
			$cols += scalar($type->cols($memdefs));
		}

		$classdef->{stateless} = !$cols
			&& (!exists $classdef->{stateless} || $classdef->{stateless});

		foreach my $base (@{$classdef->{bases}})
		{
			push @{$class_hash->{$base}{specs}}, $class;
		}
    }

    while (my ($class, $classdef) = each %$class_hash)
    {
		my $root = $class;
	
		while (@{$class_hash->{$root}{bases}})
		{
			$root = @{$class_hash->{$root}{bases}}[0];
		}

		$classdef->{root} = $class_hash->{$root};
		delete $classdef->{stateless} if $root eq $class;
    }

    return $self;
}

sub check_class
{
   my ($self, $class) = @_;
   confess "unknown class '$class'" unless exists $self->{classes}{$class};
}

sub classdef
{
   my ($self, $class) = @_;
   return $self->{classes}{$class} or confess "unknown class '$class'";
}

sub classes
{
   my ($self) = @_;
   return keys %{$self->{'classes'}};
}

sub direct_members
{
   my ($self, $class) = @_;
   return $self->{'classes'}{$class}{member_type};
}

sub all_members
{
   my ($self, $class) = @_;
   my $classes = $self->{'classes'};
	my $members = {};
   
	$self->visit_up($class, sub
	{
		my $direct_members = $classes->{shift()}{member_type};
		@$members{keys %$direct_members} = values %$direct_members;
	} );

	$members;
}

sub all_bases
{
   my ($self, $class) = @_;
   my $classes = $self->{'classes'};
	$self->visit_down($class, sub { @{ $classes->{shift()}{bases} } } );
}

sub find_member
{
   my ($self, $class, $member) = @_;
   my $classes = $self->{'classes'};
   my $result;
   local $@;

   eval
   {
      $self->visit_down($class, sub {
         die if $result = $classes->{shift()}{member_type}{$member}
         })
   };

   $result;
}

sub find_member_class
{
   my ($self, $class, $member) = @_;
   my $classes = $self->{'classes'};
   my $result;
   local $@;

   eval
   {
      $self->visit_down($class,
         sub
         {
            my $class = shift;

            if (exists $classes->{$class}{member_type}{$member})
            {
               $result = $class;
               die;
            }
         })
   };

   $result;
}

sub visit_up
{
   my ($self, $class, $fun) = @_;
   _visit_up($self, $class, $fun, { });
}

sub _visit_up
{
   my ($self, $class, $fun, $done) = @_;
   
   return if $done->{$class};

   my @results = ();

   foreach my $base (@{$self->{'classes'}{$class}{bases}})
   {
      push @results, _visit_up($self, $base, $fun, $done);
   }

   $done->{$class} = 1;

   return @results, &$fun($class);
}

sub visit_down
{
   my ($self, $class, $fun) = @_;
   _visit_down($self, $class, $fun, { });
}

sub _visit_down
{
   my ($self, $class, $fun, $done) = @_;
   
   return if $done->{$class};

   my @results = &$fun($class);

   foreach my $base (@{$self->{'classes'}{$class}{bases}})
   {
      push @results, _visit_down($self, $base, $fun, $done);
   }

   $done->{$class} = 1;

   @results
}

sub for_each_spec
{
   my ($self, $class, $fun) = @_;
   my $done = {};

   foreach my $spec (@{$self->{'classes'}{$class}{specs}})
   {
      _for_each_spec($self, $spec, $fun, $done);
   }
}

sub _for_each_spec
{
   my ($self, $class, $fun, $done) = @_;
   
   return if $done->{$class};

   &$fun($class);
   $done->{$class} = 1;

   foreach my $spec (@{$self->{'classes'}{$class}{specs}})
   {
      _for_each_spec($self, $spec, $fun, $done);
   }

}

sub declare_classes
{
   my ($self, $root) = @_;
   
   foreach my $class ($self->classes)
   {
		my $decl = "package $class;";

      my $bases = @{$self->{classes}{$class}{bases}}
         ? (join ' ', @{$self->{'classes'}{$class}{bases}})
         : $root;

		$decl .= "\@$class\:\:ISA = qw( $bases );" if $bases;

      eval $decl;
   }
}

sub is_persistent
{
   my ($self, $x) = @_;
   my $class = ref($x) || $x;
   return $self->{classes}{$class} && $self->{classes}{$class};
}

use SelfLoader;
sub DESTROY { }

1;

__DATA__

sub relational_schema
{
    my ($self) = @_;

    my $classes = $self->{classes};
    my $tables = {};

    foreach my $class (keys %{$self->{classes}})
    {
		my $classdef = $classes->{$class};
		my $tabledef = $tables->{ $classdef->{table} } ||= {};
		my $cols = $tabledef->{COLS} ||= {};

		$cols->{ $self->{sql}{id_col} } = $self->{sql}{id};
		$cols->{ $self->{sql}{class_col} } = $self->{sql}{cid} if $classdef->{root} == $classdef;

		foreach my $typetag (keys %{$classdef->{members}})
		{
			my $members = $classdef->{members}{$typetag};
			my $type = $self->{types}{$typetag};

			$type->coldefs($tabledef->{COLS}, $members, $self, $class, $tables);
		}
    }

    delete @$tables{ grep { 1 == keys %{ $tables->{$_}{COLS} } } keys %$tables };

    return bless [ $tables, $self ], 'Tangram::RelationalSchema';
}

sub Tangram::Scalar::_coldefs
{
    my ($self, $cols, $members, $sql, $schema) = @_;

    for my $def (values %$members)
    {
		$cols->{ $def->{col} } = $def->{sql} || "$sql $schema->{sql}{default_null}";
    }
}
sub Tangram::Integer::coldefs
{
    my ($self, $cols, $members, $schema) = @_;
    $self->_coldefs($cols, $members, 'INT', $schema);
}

sub Tangram::Real::coldefs
{
    my ($self, $cols, $members, $schema) = @_;
    $self->_coldefs($cols, $members, 'REAL', $schema);
}

sub Tangram::Ref::coldefs
{
    my ($self, $cols, $members, $schema) = @_;

    for my $def (values %$members)
    {
		$cols->{ $def->{col} } = !exists($def->{null}) || $def->{null}
			? "$schema->{sql}{id} $schema->{sql}{default_null}"
			: $schema->{sql}{id};
    }
}

sub Tangram::String::coldefs
{
    my ($self, $cols, $members, $schema) = @_;
    $self->_coldefs($cols, $members, 'VARCHAR(255)', $schema);
}

sub Tangram::Set::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
		$tables->{ $member->{table} }{COLS} =
		{
		 $member->{coll} => $schema->{sql}{id},
		 $member->{item} => $schema->{sql}{id},
		};
    }
}

sub Tangram::IntrSet::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
		my $table = $tables->{ $schema->{classes}{$member->{class}}{table} } ||= {};
		$table->{COLS}{$member->{coll}} = "$schema->{sql}{id} $schema->{sql}{default_null}";
    }
}

sub Tangram::Array::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
		$tables->{ $member->{table} }{COLS} =
		{
		 $member->{coll} => $schema->{sql}{id},
		 $member->{item} => $schema->{sql}{id},
		 $member->{slot} => "INT $schema->{sql}{default_null}"
		};
    }
}

sub Tangram::Hash::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
		$tables->{ $member->{table} }{COLS} =
		{
		 $member->{coll} => $schema->{sql}{id},
		 $member->{item} => $schema->{sql}{id},
		 $member->{slot} => "VARCHAR(255) $schema->{sql}{default_null}"
		};
    }
}

sub Tangram::IntrArray::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
		my $table = $tables->{ $schema->{classes}{$member->{class}}{table} } ||= {};
		$table->{COLS}{$member->{coll}} = "$schema->{sql}{id} $schema->{sql}{default_null}";
		$table->{COLS}{$member->{slot}} = "INT $schema->{sql}{default_null}";
    }
}

sub Tangram::HashRef::coldefs
{
    #later
}

sub Tangram::BackRef::coldefs
{
    return ();
}

package Tangram::Schema;

sub deploy
{
	my ($self, $out) = @_;
    $self->relational_schema()->deploy($out);
}

sub retreat
{
	my ($self, $out) = @_;
    $self->relational_schema()->retreat($out);
}

package Tangram::RelationalSchema;

sub _deploy_do
{
    my $output = shift;

    return ref($output) && eval { $output->isa('DBI::db') }
		? sub { print $Tangram::TRACE @_, "\n" if $Tangram::TRACE;
			$output->do( join '', @_ ); }
		: sub { print $output @_, ";\n\n" };
}

sub retreat
{
    my ($self, $output) = @_;
    my ($tables, $schema) = @$self;

    $output ||= \*STDOUT;

    my $do = _deploy_do($output);

    for my $table (sort keys %$tables, $schema->{class_table}, $schema->{control})
    {
		$do->( "DROP TABLE $table" );
    }
}

sub deploy
{
    my ($self, $output) = @_;
    my ($tables, $schema) = @$self;

    $output ||= \*STDOUT;

    my $do = _deploy_do($output);

    foreach my $table (sort keys %$tables)
    {
		my $def = $tables->{$table};
		my $cols = $def->{COLS};

		my @base_cols;

		my $id_col = $schema->{sql}{id_col};
		my $class_col = $schema->{sql}{class_col};

		push @base_cols, "$id_col $schema->{sql}{id} NOT NULL,\n  PRIMARY KEY( id )" if exists $cols->{$id_col};
		push @base_cols, "$class_col $schema->{sql}{cid} NOT NULL" if exists $cols->{$class_col};

		delete @$cols{$id_col};
		delete @$cols{$class_col};

		$do->("CREATE TABLE $table\n(\n  ",
			  join( ",\n  ", @base_cols, map { "$_ $cols->{$_}" } keys %$cols ),
			  "\n)" );
    }

my $control = $schema->{control};
	
    $do->( <<SQL );
CREATE TABLE $control
(
major INTEGER NOT NULL,
minor INTEGER NOT NULL,
mark INTEGER NOT NULL
)
SQL

my ($major, $minor) = split '\.', $Tangram::VERSION;

    $do->("INSERT INTO $control (major, minor, mark) VALUES ($major, $minor, 0)");
}

sub classids
{
    my ($self) = @_;
    my ($tables, $schema) = @$self;
	my $classes = $schema->{classes};
	use Data::Dumper;
	return { map { $_ => $classes->{$_}{id} } keys %$classes };
}

1;

