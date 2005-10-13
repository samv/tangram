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
use Tangram::Relational::Schema;
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

    return bless [ $tables, $self ], 'Tangram::Relational::Schema';
}

#---------------------------------------------------------------------
#  Tangram::Type::Integer->coldefs($cols, $members, $schema)
#  Tangram::Type::Real->coldefs($cols, $members, $schema)
#  Tangram::Type::String->coldefs($cols, $members, $schema)
#
# Setup column defines for the root column types
#---------------------------------------------------------------------
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
    my $driver = $engine->{driver} || Tangram::Relational->new();

    my $do = _deploy_do($output);

    foreach my $table (sort keys %$tables)
    {
	my $def = $tables->{$table};
	my $cols = $def->{COLS};

	my @base_cols;

	my $type = $def->{TYPE} || $schema->{sql}{table_type};

	my $id_col = $schema->{sql}{id_col};
	my $class_col = $schema->{sql}{class_col} || 'type';
	my $timestamp_col = $schema->{sql}{timestamp_col} || '__ts';
	my $timestamp_type = $schema->{sql}{timestamp} || 'TIMESTAMP';
	my $timestamp = $schema->{sql}{timestamp_all_tables};

	push @base_cols,("$id_col ".
			 $driver->type("$schema->{sql}{id} NOT NULL"))
	    if exists $cols->{$id_col};
	push @base_cols, "$class_col "
	    .$driver->type("$schema->{sql}{cid} NOT NULL")
	    if exists $cols->{$class_col};

	push @base_cols, "$timestamp_col "
	    .$driver->type("$timestamp_type NOT NULL")
            if $timestamp;

	delete @$cols{$id_col};
	delete @$cols{$class_col};

	$do->("CREATE TABLE $table\n(\n  ",
	      join( ",\n  ", (@base_cols,
			      map { "$_ ".$driver->type($cols->{$_}) }
			      keys %$cols),
		    ( exists $cols->{$id_col} 
		      ? ("PRIMARY KEY( $id_col )")
		      : () ),
		  ),
	      "\n) ".($type?" TYPE=$type":""));

    }

    my %made_sequence;

    foreach my $class ( values %{$schema->{classes}} ) {
	if ( my $sequence = $class->{oid_sequence} ) {
	    $do->($driver->mk_sequence_sql($sequence))
		unless $made_sequence{$sequence}++;
	}
    }

    my $control = $schema->{control};
    my $table_type = $schema->{sql}{table_type};

    if ( my $sequence = $schema->{sql}{oid_sequence} ) {

	$do->($driver->mk_sequence_sql($sequence))
	    unless $made_sequence{$sequence}++;

    } else {
    $do->( <<SQL . ($table_type?" TYPE=$table_type":"") );
CREATE TABLE $control
(
layout INTEGER NOT NULL,
engine VARCHAR(255),
engine_layout INTEGER,
mark INTEGER NOT NULL
)
SQL

    my $info = $engine->get_deploy_info();
    #my ($l) = split '\.', $Tangram::VERSION;

    # Prevent additional records on redeploy.
    #  -- ks.perl@kurtstephens.com 2004/04/29
    $do->("CREATE UNIQUE INDEX ${control}_Guard ON $control (layout, engine, engine_layout)");

    $do->("INSERT INTO $control (layout, engine, engine_layout, mark)"
	  ." VALUES ($info->{LAYOUT}, '$info->{ENGINE}', "
	  ."$info->{ENGINE_LAYOUT}, 0)");

    }
}

sub retreat
{
    my ($self, $output) = @_;
    my ($tables, $engine) = @$self;
	my $schema = $engine->{SCHEMA};

    $output ||= \*STDOUT;

    my $do = _deploy_do($output);

    my %dropped_sequences;
    my $driver = $engine->{driver} || Tangram::Relational->new();

    my $oid_sequence = $schema->{sql}{oid_sequence};
    for my $table (sort keys %$tables,
		   ($oid_sequence ? () : $schema->{control}))
    {
		$do->( "DROP TABLE $table" );
    }

    for my $class ( values %{ $schema->{classes} } ) {
	if ( my $sequence = $class->{oid_sequence} ) {
	    $do->($driver->drop_sequence_sql($sequence))
		unless $dropped_sequences{$sequence}++;
	}
    }

    if ( $oid_sequence ) {
	$do->($driver->drop_sequence_sql($oid_sequence));
    }
}

# XXX - never reached in the test suite; debugging function?
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

use Set::Object;

use vars qw($paren);
$paren = qr{\( (?: (?> [^()]+ )    # Non-parens without backtracking
	    |   (??{ $paren })  # Group with matching parens
	   )* \)}x;


sub instantiate {

    my ($self, $remote, $xcols, $xfrom, $xwhere, %o) = @_;
    my ($expand, $cols, $from, $where) = @$self;

    $xcols ||= [];
    $xfrom ||= [];

    my @xwhere;

    if (@$xwhere) {
	$xwhere[0] = join ' AND ', @$xwhere;
	$xwhere[0] =~ s[%][%%]g;
    }

    my @tables = $remote->table_ids() if $remote;

    # expand table aliases early
    my $i = 0;
    my @cols = map { sprintf $_, map { $tables[$expand->[$i++]] } m{(%d)}g } @$cols;
    my @from = map { sprintf $_, map { $tables[$expand->[$i++]] } m{(%d)}g } @$from;
    my @where = map { sprintf $_, map { $tables[$expand->[$i++]] } m{(%d)}g } @$where;

    my $selected;
    if ( my $group = $o{group} ) {
	# grouping, (make sure that all columns are aggregate)

	# make sure all grouped columns are selected
	$selected = Set::Object->new(@cols, @$xcols);

	push @$xcols, (grep { $selected->insert($_) }
		       map { ref $_ ? $_->expr : $_ } @$group);
    }

    my @order;
    if ($o{order}) {
	if ( ref($o{order}) eq 'ARRAY') {
	    @order = map {ref $_ ? $_->expr : $_} @{$o{order}};
	} else {
	    @order = (ref $o{order} ? $o{order}->expr : $o{order});
	}
    }

    if (@order) {
	# ordering, make sure that all ordered columns are selected
	$selected ||= Set::Object->new(@cols, @$xcols);

        push @$xcols, (grep { $selected->insert($_) } @order);
    }

    my $select = sprintf("SELECT%s\n%s\n",
			 ($o{distinct} ? " DISTINCT" : ""),
			 (join(",\n", map {"    $_"} @cols, @$xcols)));

    # add outer join clauses
    if ( my $owhere = $o{owhere} ) {

	#kill 2, $$;
	my $ofrom = $o{ofrom};

	# ugh.  we need to add a new clause for every join, and in
	# order of joinedness.  Which means that we have to go and
	# break up some joins.

	# this is highly ugly, but at least it makes something that
	# was impossible, possible.  This requires a thorough
	# re-engineering to fix, as I see it.

	$owhere = Set::Object->new(map {
	    my @x;
	    while ( s{^\(((?:[^(]+|$paren)*)\s+and\s((?:[^(]+|$paren)*)\)$}{$1}is
		    or s{^((?:[^(]+|$paren)*)\s+and\s((?:[^(]+|$paren)*)$}{$1}is
		  ) {
		#print STDERR "got: $2\n";
		push @x, $2;
	    }
	    #print STDERR "left: $_\n";
	    @x, $_
	} @{$o{owhere}});
	#print STDERR "new owhere: ".join("/",$owhere->members)."\n";
	#print STDERR "ofrom: @$ofrom\n";
	$ofrom = Set::Object->new(@{$o{ofrom}});
	#print STDERR "new ofrom: ".join("/",$ofrom->members)."\n";

	(my $tmp_sel = $select) =~ s{.*^FROM}{}ms;

	# ook ook
	my $seen_from = Set::Object->new( map { m{\b(tl?\d+)\b}sg }
					  (@from, @$xfrom) );

	my (@ofrom, @ojoin, %owhen);

	# this loop is heinous
	while ( $ofrom->size ) {
	FROM:
	    my $ofrom_size = $ofrom->size;
	    my @from_todo = $ofrom->members;

	    while ( my $from = shift @from_todo ) {
		my ($tnum) = ($from =~ m{\b(tl?\d+)\b})
		    or die "What? `$from; doesn't m/tl?\\d+/ ?";
		my @tmpjoin;

		#print STDERR "Checking (outer): $from\n";
		my @queue = $owhere->members;
	    JOIN:
		while ( my $join  = shift @queue ) {
		    my @tables = ($join =~ m{\b(tl?\d+)\b}g);
		    next unless ( grep { $_ eq $tnum } @tables );
		    #print STDERR "Checking: $join for @tables (seen_from = $seen_from)\n";
		    if ( my @bad = grep { !$seen_from->has($_)
					     and $_ ne $tnum 
				      } @tables ) {
			next JOIN;
		    } else {
			my (@others) = (grep { $_ ne $tnum } @tables);
			(@others == 1)
			    or die("Can't handle more than two-table "
				   ."outer join clauses");

			$owhen{$others[0]} = scalar @ofrom;
			#print STDERR "ADDED JOIN FROM $others[0] to $tnum ($from?): $join\n";

			# hooray!  SQL will accept it in this order!
			$seen_from->insert($tnum);
			#print STDERR "SEEN ADDED: $tnum\n";
			$ofrom-= Set::Object->new($from);
			#print STDERR "OFROM REMOVED: $from\n";
			$owhere-= Set::Object->new($join);

			# we're joining in $from, so add all clauses
			# that have nothing but seen tables and from
			@queue = $owhere->members;
			@from_todo = $ofrom->members;

			push(@tmpjoin, $join);
		    }
		}
		if ( @tmpjoin ) {
		    push @ofrom, $from;
		    push @ojoin, \@tmpjoin;
		}
	    }
	    die "failed to join tables: ".join(", ", $ofrom->members)
		."\nquery: >-\n$select\nowhere:\n".join(", ", $owhere->members)
		    ."supplied from:\n"
			.join(", ", @from, @$xfrom)
		if $ofrom->size;
	}
	die "failed to include conditions: ".join(", ", $owhere->members)
	    if $owhere->size;

	my @tables = (@from, @$xfrom);

	for my $table ( @tables ) {
	    my ($tnum) = ($table =~ m/\b(tl?\d+)\b/)
		or die "table without an alias";

	    while ( defined(my $idx = delete $owhen{$tnum}) ) {
		my $from = $ofrom[$idx];
		my $join = $ojoin[$idx];
		$ofrom[$idx] = undef;
		$table .= (sprintf
			   ("\n\tLEFT OUTER JOIN\n%s\n\tON\n%s",
			    join(",\n", map { "\t    $_" } $from),
			    join("\tAND\n", map { "\t    $_" } @$join),
			   ));
		($tnum) = grep { $_ ne $tnum } ($from =~ m/\b(tl?\d+)\b/g);
	    }
	}
	if ( my @missed = grep { defined } @ofrom ) {
	    die "Couldn't figure out where to stick @missed";
	}
	$select .= sprintf ("FROM\n%s\n",
			    (join(",\n", map {"    $_"} @tables))
			   );
    } else {
	$select .= sprintf ("FROM\n%s\n",
			    (join(",\n", map {"    $_"} @from, @$xfrom))
			   );
    }

    my $max_len = 0;

    #push @xwhere, @{$o{lwhere}} if $o{lwhere};

    foreach (@where, @xwhere) {

	if ( $Tangram::TRACE and $Tangram::DEBUG_LEVEL <= 1) {
	    # In trace mode, split up queries that have an AND clause
	    # but no parantheses.  be sure not to put parantheses in
	    # hardcoded queries, inside quotes etc.
	    while (my ($left, $right) =
		   m/^((?:[^(]+|$paren)*)\s+and\s((?:[^(]+|$paren)*)$/i) {
		$_ = $left;
		push @xwhere, $right;
	    }
	}
	($max_len = length $_) if (length $_ > $max_len);
    }
    # don't go insane with the spaces!
    $max_len = 20 if $max_len > 20;

    $select .= sprintf("WHERE\n%s\n",
		       join("    AND\n", map {
			   sprintf("    %-${max_len}s", $_)
		       } @where, @xwhere)
		      )
	if @where || @$xwhere;

    if ( my $group = $o{group} ) {
	$select .= ("GROUP BY\n".
		    join ",\n", map { "    ".$_->expr } @$group)."\n";
    }

    if (@order) {
	my $desc = $o{desc};
	if ( ! ref $desc ) {
	    $desc = [ ($desc) x @order ];
	}
	my $i = 0;
	$select .= "ORDER BY\n".
	    join(",\n", (map { ("    ".$_.
				($desc->[$i++] ? " DESC" : "")) }
			 @order))."\n";
    }

    if (defined $o{limit}) {
	if (ref $o{limit}) {
	    $select .= "LIMIT\n    ".join(",",@{ $o{limit} })."\n";
	} else {
	    $select .= "LIMIT\n    $o{limit}\n";
	}
    }

    if ( defined $o{postfilter} ) {
	$select = "SELECT\n    *\nFROM\n(\n$select\n)\n"
	    .sprintf("WHERE\n%s\n",
		     join("    AND\n", map {
			 sprintf("    %-${max_len}s", $_)
		     } @{$o{postfilter}}
			 )
		    );
    }

    $select;
    #sprintf $select, map { $tables[$_] } @$expand;
}

sub extract {

    my ($self, $row) = @_;
    my $id = shift @$row;
    my $class_id = shift @$row;

    my $slice = $self->[-1]{$class_id}
	or do {
	    kill 2, $$;
	    Carp::croak("unexpected class id '$class_id' (OK: "
			.(join(",",keys %{$self->[-1]})).")");
	};

    my $state = [ @$row[ @$slice ] ];

    splice @$row, 0, @{ $self->[1] } - 2;

    return ($id, $class_id, $state);
}

1;

#########################################################################
#########################################################################
#########################################################################
#########################################################################
#########################################################################
#########################################################################

package Tangram::Relational::Engine;

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
