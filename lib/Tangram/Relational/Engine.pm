#=====================================================================
#  Tangram::Relational::Engine
#
# Broom, broom!
#
# Each Class has an Engine, which generates closures to do certain
# operations.  This engine is generated from the Tangram Schema (?)
#
# The operations are:
#
#   - get_heterogeniety() - the total number of subclasses of a given
#                           class, I think
#
# A whole load forwarded to the Tangram::Relational::Engine::Class
# object:
#
#   - get_instance_select
#   - get_insert_statements
#   - get_insert_fields
#   - get_update_statements
#   - get_update_fields
#   - get_deletes
#   - get_polymorphic_select
#   - get_table_set
#
#   - get_save_cache (?)
#   - qualify
#
# Generated;
#
#   - get_exporter
#   - get_importer
#=====================================================================
package Tangram::Relational::Engine;

use strict;
use Tangram::Schema;
use Tangram::Relational::TableSet;
use Tangram::RelationalSchema;
use Tangram::Relational::PolySelectTemplate;
use Tangram::Relational::Engine::Class;


sub new {
    my ($class, $schema, %opts) = @_;

    my $heterogeneity = { };
    my $engine = bless { SCHEMA => $schema,
			 HETEROGENEITY => $heterogeneity }, $class;

    if ($opts{layout1}) {
	$engine->{layout1} = 1;
	$engine->{TYPE_COL} = $schema->{sql}{class_col} || 'classId';
    } else {
	$engine->{TYPE_COL} = $schema->{sql}{class_col} || 'type';
    }

    if ( $opts{driver} ) {
	$engine->{driver} = $opts{driver};
	print $Tangram::TRACE ref($opts{driver})." driver selected\n"
	    if $Tangram::TRACE;
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
	    ++$heterogeneity->{$key}
		if my $ss = ($engine->get_table_set($other)
			     ->is_improper_superset($table_set));
	    my $other_key = $engine->get_table_set($other)->key;
	}
    }

    # use Data::Dumper; print Dumper $heterogeneity;

    return $engine;
}

sub get_heterogeneity {
    my ($self, $table_set) = @_;
    my $key = $table_set->key();

    return $self->{HETEROGENEITY}{$key} ||= do {

	# XXX - this code path never reached in the test suite - is it
	# required?
	my $heterogeneity = 0;

	for my $class (values %{ $self->{CLASS} }) {
	    ++$heterogeneity
		if (!$class->{abstract} &&
		    ($class->get_table_set($self)
		     ->is_improper_superset($table_set)));
	}

	$heterogeneity;
    }
}

sub get_parts
  {
	my ($self, $class) = @_;

	@{ $self->{CLASSES}{$class->{name}}{PARTS} ||= do {
	  my %seen;
	  [ grep { !$seen{ $_->{name} }++ }
		(map { $self->get_parts($_) } $class->direct_bases()),
		$class
	  ]
	} }
  }

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

sub get_deploy_info
  {
	my ($self) = @_;
	return { LAYOUT => 2, ENGINE => ref($self), ENGINE_LAYOUT => 1 };
  }

sub relational_schema
  {
    my ($self) = @_;

    my $schema = $self->{SCHEMA};
    my $classes = $schema->{classes};
    my $tables = {};

    foreach my $class (keys %{$schema->{classes}}) {

	my $classdef = $classes->{$class};

	my $tabledef = $tables->{ $classdef->{table} } ||= {};
	my $cols = $tabledef->{COLS} ||= {};
	$tabledef->{TYPE} = $classdef->{table_type};

	$cols->{ $schema->{sql}{id_col} } = $schema->{sql}{id};

	$cols->{ $schema->{sql}{class_col} || 'type' }
	    = $schema->{sql}{cid}
		if $self->{ROOT_TABLES}{$classdef->{table}};

	foreach my $typetag (keys %{$classdef->{members}})
	{
	    my $members = $classdef->{members}{$typetag};
	    my $type = $schema->{types}{$typetag};

	    $type->coldefs($tabledef->{COLS}, $members, $schema,
			   $class, $tables);
	}
    }

    delete @$tables{
		    grep { 1 == keys %{ $tables->{$_}{COLS} } }
		    keys %$tables
		   };

    return bless [ $tables, $self ], 'Tangram::RelationalSchema';
}

#---------------------------------------------------------------------
#  Tangram::Integer->coldefs($cols, $members, $schema)
#  Tangram::Real->coldefs($cols, $members, $schema)
#  Tangram::String->coldefs($cols, $members, $schema)
#
# Setup column defines for the root column types
#---------------------------------------------------------------------
sub Tangram::Integer::coldefs
{
    my ($self, $cols, $members, $schema) = @_;
    $self->_coldefs($cols, $members, 'INT', $schema);
}

sub Tangram::String::coldefs
{
    my ($self, $cols, $members, $schema) = @_;
    $self->_coldefs($cols, $members, 'VARCHAR(255)', $schema);
}

#---------------------------------------------------------------------
#  Tangram::Set->coldefs($cols, $members, $schema, $class, $tables)
#
#  Setup column mappings for many to many unordered mappings (link
#  table)
#---------------------------------------------------------------------
sub Tangram::Set::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
	my $COLS = $tables->{ $member->{table} }{COLS} ||= { };

	$COLS->{$member->{coll}} = $schema->{sql}{id};
	$COLS->{$member->{item}} = $schema->{sql}{id};
    }
}

#---------------------------------------------------------------------
#  Tangram::IntrSet->coldefs($cols, $members, $schema, $class,
#                            $tables)
#
#  Setup column mappings for one to many unordered mappings (foreign
#  key)
#---------------------------------------------------------------------
sub Tangram::IntrSet::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
	my $table =
	    $tables->{ $schema->{classes}{$member->{class}}{table} }
		||= {};
	$table->{COLS}{$member->{coll}}
	    = "$schema->{sql}{id} $schema->{sql}{default_null}";
    }
}

#---------------------------------------------------------------------
#  Tangram::Array->coldefs($cols, $members, $schema, $class, $tables)
#
#  Setup column mappings for many to many unordered mappings (link
#  table with integer category)
#---------------------------------------------------------------------
sub Tangram::Array::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
	my $COLS = $tables->{ $member->{table} }{COLS} ||= { };

	$COLS->{$member->{coll}} = $schema->{sql}{id};
	$COLS->{$member->{item}} = $schema->{sql}{id};
	$COLS->{$member->{slot}} = "INT $schema->{sql}{default_null}";
    }
}

#---------------------------------------------------------------------
#  Tangram::IntrArray->coldefs($cols, $members, $schema, $class,
#                              $tables)
#
#  Setup column mappings for one to many ordered mappings (foreign
#  key with associated integer category/column)
#---------------------------------------------------------------------
sub Tangram::IntrArray::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members) {
	my $table =
	    $tables->{ $schema->{classes}{$member->{class}}{table} }
		||= {};
	$table->{COLS}{$member->{coll}}
	    = "$schema->{sql}{id} $schema->{sql}{default_null}";
	$table->{COLS}{$member->{slot}}
	    = "INT $schema->{sql}{default_null}";
    }
}

#---------------------------------------------------------------------
#  Tangram::Hash->coldefs($cols, $members, $schema, $class, $tables)
#
#  Setup column mappings for many to many indexed mappings (link
#  table with string category)
#---------------------------------------------------------------------
sub Tangram::Hash::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
	$tables->{ $member->{table} }{COLS} =
	    {
	     $member->{coll} => $schema->{sql}{id},
	     $member->{item} => $schema->{sql}{id},
	     # XXX - hardcoded slot type
	     $member->{slot} => "VARCHAR(255) $schema->{sql}{default_null}"
	    };
    }
}

#---------------------------------------------------------------------
#  Tangram::IntrHash->coldefs($cols, $members, $schema, $class,
#                             $tables)
#
#  Setup column mappings for one to many indexed mappings (foreign
#  key with string category)
#---------------------------------------------------------------------
sub Tangram::IntrHash::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
	my $table =
	    $tables->{ $schema->{classes}{$member->{class}}{table} }
		||= {};
	$table->{COLS}{$member->{coll}} =
	    "$schema->{sql}{id} $schema->{sql}{default_null}";
	$table->{COLS}{$member->{slot}} =
	    "VARCHAR(255) $schema->{sql}{default_null}";
    }
}

#---------------------------------------------------------------------
#  Tangram::BackRef->coldefs(...)
#
#  BackRefs do not set up any columns by default.
#---------------------------------------------------------------------
sub Tangram::BackRef::coldefs
{
    return ();
}

#---------------------------------------------------------------------
#  $engine->get_class_engine($ClassDef)
#
#  Returns the Engine for a particular Class - the class definition is
#  passed rather than the name.
#
#  Returns a Tangram::Relational::Engine::Class object.
#---------------------------------------------------------------------
sub get_class_engine {
  my ($engine, $class) = @_;

  my $class_engine;

  unless ($class_engine = $engine->{CLASS}{$class->{name}}) {

      $class_engine = $engine->{CLASS}{$class->{name}}
	  = $engine->make_class_engine($class);

      $class_engine->initialize($engine, $class, $class);
  }

  return $class_engine;
}

#---------------------------------------------------------------------
#  $engine->make_class_engine($ClassDef)
#---------------------------------------------------------------------
sub make_class_engine {
    my ($self, $class) = @_;
    return Tangram::Relational::Engine::Class->new();
}

# forward some methods to class engine

for my $method (qw( get_instance_select
		    get_insert_statements get_insert_fields
		    get_update_statements get_update_fields
		    get_deletes
		    get_polymorphic_select get_table_set
		  )) {
    eval qq{
	sub $method {
	    my (\$self, \$class, \@args) = \@_;
	    return \$self->get_class_engine(\$class)->$method(\$self, \@args);
	}
    }
}

#---------------------------------------------------------------------
#  $engine->get_exporter($ClassDef)
#
# Returns a closure that will `export' an object to the DB
# XXX - never reached in the test suite
#---------------------------------------------------------------------
sub get_exporter {
    my ($self, $class) = @_;
    return $self->get_class_engine($class)->get_exporter
	( { layout1 => $self->{layout1} } );
}

#---------------------------------------------------------------------
#  $engine->get_importer($ClassDef)
#
# Returns a closure that will `import' an object from the DB
# XXX - never reached in the test suite
#---------------------------------------------------------------------
sub get_importer {
    my ($self, $class) = @_;
    return $self->get_class_engine($class)->get_importer
	( { layout1 => $self->{layout1} } );
}

# Looks like a Catch 22 destructor, but test suite says otherwise :)
sub DESTROY {
    my ($self) = @_;

    for my $class (values %{ $self->{CLASS} }) {
	$class->fracture()
	    if $class;
    }
}

1;
