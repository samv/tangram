# (c) Sound Object Logic 2000-2001

use strict;

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

	  for my $part ($self->get_parts($class)) {
		my $table_name = $part->{table};

		$context->{class} = $part;

		my $table = $tables{$table_name} ||= do { push @tables, my $table = [ $table_name, [], [] ]; $table };
		
		for my $field ($part->direct_fields()) {
		  
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
	  
	  for my $part ($self->get_parts($class)) {
		my $table = $part->{table};
		$context->{class} = $part;
		push @cols, map { "$table.$_" } map { $_->get_import_cols($context) } $part->direct_fields()
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
		  
		  for my $part ($self->get_parts($class)) {
			my $table = $part->{table};
			$context->{class} = $part;
			
			for my $field ($part->direct_fields()) {
			  my @import_cols = $field->get_import_cols($context);
			  $used{$table} += @import_cols;

			  for my $col (@import_cols) {
				my $qualified_col = "$table.$col";
				unless (exists $col_index{$qualified_col}) {
				  push @cols, qualify($col, $table, \%base_tables, \@expand);
				  $col_index{$qualified_col} = $col_mark++;
				}

				push @slice, $col_index{$qualified_col};
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
		
		for my $part ($self->get_parts($class)) {
		  my $table_name = $part->{table};
		  
		  $context->{class} = $part;

		  for my $field ($part->direct_fields) {
			
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
	  
	  $cols->{ $schema->{sql}{id_col} } = $schema->{sql}{id};

	  $cols->{ $schema->{sql}{class_col} || 'type' } = $schema->{sql}{cid} if $self->{ROOT_TABLES}{$classdef->{table}};
	  
	  foreach my $typetag (keys %{$classdef->{members}})
		{
		  my $members = $classdef->{members}{$typetag};
		  my $type = $schema->{types}{$typetag};
		  
		  $type->coldefs($tabledef->{COLS}, $members, $schema, $class, $tables);
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

# sub Tangram::Ref::coldefs
# {
#     my ($self, $cols, $members, $schema) = @_;

#     for my $def (values %$members)
#     {
# 		$cols->{ $def->{col} } = !exists($def->{null}) || $def->{null}
# 			? "$schema->{sql}{id} $schema->{sql}{default_null}"
# 			: $schema->{sql}{id};
#     }
# }

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

package Tangram::RelationalSchema;

sub _deploy_do
{
    my $output = shift;

    return ref($output) && eval { $output->isa('DBI::db') }
		? sub { print $Tangram::TRACE @_, "\n" if $Tangram::TRACE;
			$output->do( join '', @_ ); }
		: sub { print $output @_, ";\n\n" };
}

sub deploy
{
    my ($self, $output) = @_;
    my ($tables, $engine) = @$self;
	my $schema = $engine->{SCHEMA};

    $output ||= \*STDOUT;

    my $do = _deploy_do($output);

    foreach my $table (sort keys %$tables)
    {
		my $def = $tables->{$table};
		my $cols = $def->{COLS};

		my @base_cols;

		my $id_col = $schema->{sql}{id_col};
		my $class_col = $schema->{sql}{class_col} || 'type';

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
layout INTEGER NOT NULL,
engine VARCHAR(255),
engine_layout INTEGER,
mark INTEGER NOT NULL
)
SQL

	my $info = $engine->get_deploy_info();
my ($l) = split '\.', $Tangram::VERSION;

    $do->("INSERT INTO $control (layout, engine, engine_layout, mark) VALUES ($info->{LAYOUT}, '$info->{ENGINE}', $info->{ENGINE_LAYOUT}, 0)");
}

sub retreat
{
    my ($self, $output) = @_;
    my ($tables, $engine) = @$self;
	my $schema = $engine->{SCHEMA};

    $output ||= \*STDOUT;

    my $do = _deploy_do($output);

    for my $table (sort keys %$tables, $schema->{control})
    {
		$do->( "DROP TABLE $table" );
    }
}

sub classids
{
    my ($self) = @_;
    my ($tables, $schema) = @$self;
	my $classes = $schema->{classes};
	# use Data::Dumper;
	return { map { $_ => $classes->{$_}{id} } keys %$classes };
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
