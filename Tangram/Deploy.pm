use strict;
use Carp;

package Tangram::Schema;

my $id_type = 'numeric(15, 0)';
my $oid_type = 'numeric(10, 0)';
my $cid_type = 'numeric(5,0)';
my $classname_type = 'varchar(128)';

sub relational_schema
{
    my ($self, $file) = @_;

    my $classes = $self->{classes};
    my $tables = {};

    foreach my $class (keys %{$self->{classes}})
    {
	my $classdef = $classes->{$class};
	my $tabledef = $tables->{$class} ||= {};
	my $cols = $tabledef->{COLS} ||= {};

	$cols->{id} = $id_type;
	$cols->{classId} = $cid_type if $classdef->{root} == $classdef;

	foreach my $typetag (keys %{$classdef->{members}})
	{
	    my $members = $classdef->{members}{$typetag};
	    my $type = $self->{types}{$typetag};

	    $type->coldefs($tabledef->{COLS}, $members, $self, $class, $tables);
	    # @{$tabledef->{COLS}}{ $type->cols($members) } = $type->coldefs($members, $self, $class, $tables);
	}
    }

    delete @$tables{ grep { 1 == keys %{ $tables->{$_}{COLS} } } keys %$tables };

    return bless [ $tables, $self ], 'Tangram::RelationalSchema';
}

sub Tangram::Scalar::_coldefs
{
    my ($self, $cols, $members, $sql) = @_;

    for my $def (values %$members)
    {
	$cols->{ $def->{col} } = $def->{sql} || $sql;
    }
}

sub Tangram::Integer::coldefs
{
    my ($self, $cols, $members) = @_;
    $self->_coldefs($cols, $members, 'INT NULL');
}

sub Tangram::Real::coldefs
{
    my ($self, $cols, $members) = @_;
    $self->_coldefs($cols, $members, 'REAL NULL');
}

sub Tangram::Ref::coldefs
{
    my ($self, $cols, $members) = @_;

    for my $def (values %$members)
    {
	$cols->{ $def->{col} } = !exists($def->{null}) || $def->{null} ? "$id_type NULL" : $id_type;
    }
}

sub Tangram::String::coldefs
{
    my ($self, $cols, $members) = @_;
    $self->_coldefs($cols, $members, 'VARCHAR(255) NULL');
}

sub Tangram::Set::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (keys %$members)
    {
	$tables->{ $members->{$member}{table} }{COLS} =
	{ coll => $id_type, item => $id_type };
    }
}

sub Tangram::IntrSet::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
	my $table = $tables->{ $schema->{classes}{$member->{class}}{table} } ||= {};
	$table->{COLS}{$member->{coll}} = "$id_type NULL";
    }
}

sub Tangram::Array::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (keys %$members)
    {
	$tables->{ $members->{$member}{table} }{COLS} =
	{ coll => $id_type, item => $id_type, slot => 'INT NULL' };
    }
}

sub Tangram::Hash::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (keys %$members)
    {
	$tables->{ $members->{$member}{table} }{COLS} =
	{ coll => $id_type, item => $id_type, slot => 'VARCHAR(128)' };
    }
}

sub Tangram::IntrArray::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
	my $table = $tables->{ $schema->{classes}{$member->{class}}{table} } ||= {};
	$table->{COLS}{$member->{coll}} = "$id_type NULL";
	$table->{COLS}{$member->{slot}} = 'INT NULL';
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
    shift->relational_schema()->deploy(@_);
}

sub retreat
{
    shift->relational_schema()->retreat(@_);
}

package Tangram::RelationalSchema;

sub _deploy_do
{
    my $output = shift;

    return ref($output) && eval { $output->isa('DBI::db') }
    ? sub { $output->do( join '', @_ ) }
    : sub { print $output @_, ";\n\n" };
}


sub retreat
{
    my ($self, $output) = @_;
    my ($tables, $schema) = @$self;

    my $do = _deploy_do($output);

    for my $table (sort keys %$tables, $schema->{class_table})
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

	push @base_cols, "id $id_type NOT NULL,\n  PRIMARY KEY( id )" if exists $cols->{id};
	push @base_cols, "classId $cid_type NOT NULL" if exists $cols->{classId};

	delete @$cols{qw( id classId )};

	$do->("CREATE TABLE $table\n(\n  ",
	      join( ",\n  ", @base_cols, map { "$_ $cols->{$_}" } keys %$cols ),
	      "\n)" );
    }

    $do->( <<SQL );
CREATE TABLE $schema->{class_table}
(
 classId $cid_type NOT NULL,
 className $classname_type,
 lastObjectId $oid_type,
 PRIMARY KEY ( classId )
)
SQL

    my $cids = $self->classids();
    $do->("INSERT INTO OpalClass(classId, className, lastObjectId) VALUES ($cids->{$_}, '$_', 0)" )
        for keys %$cids;
}

sub retreat
{
    my ($self, $output) = @_;

    my $do = _deploy_do($output);

    for my $table (sort keys %$self, 'OpalClass')
    {
	$do->( "DROP TABLE $table" );
    }
}

sub classids
{
    my ($self) = @_;
    my ($tables, $schema) = @$self;

    my $classes = $schema->{classes};
    my $classids = {};
    my $classid = 1;

    foreach my $class (keys %$classes)
    {
	$classids->{$class} = $classid++ unless $classes->{$class}{abstract};
    }

    return $classids;
}

1;
