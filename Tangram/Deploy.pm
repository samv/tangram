use strict;
use Carp;

package Tangram::Schema;

#my $id_type = 'numeric(15, 0)';
#my $oid_type = 'numeric(10, 0)';
#my $cid_type = 'numeric(5,0)';
my $classname_type = 'varchar(128)';

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

		$cols->{id} = $self->{sql}{id};
		$cols->{classId} = $self->{sql}{cid} if $classdef->{root} == $classdef;

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

		push @base_cols, "id $schema->{sql}{id} NOT NULL,\n  PRIMARY KEY( id )" if exists $cols->{id};
		push @base_cols, "classId $schema->{sql}{cid} NOT NULL" if exists $cols->{classId};

		delete @$cols{qw( id classId )};

		$do->("CREATE TABLE $table\n(\n  ",
			  join( ",\n  ", @base_cols, map { "$_ $cols->{$_}" } keys %$cols ),
			  "\n)" );
    }

    $do->( <<SQL );
CREATE TABLE $schema->{class_table}
(
 classId $schema->{sql}{cid} NOT NULL,
 className $classname_type,
 lastObjectId $schema->{sql}{oid},
 PRIMARY KEY ( classId )
)
SQL

    my $cids = $self->classids();
    foreach (keys %$cids) {
      $do->("INSERT INTO $schema->{class_table}(classId, className, lastObjectId) VALUES ($cids->{$_}, '$_', 0)" );
    }
	
      #  for keys %$cids;
}

sub classids
{
    my ($self) = @_;
    my ($tables, $schema) = @$self;

    my $classes = $schema->{classes};
    my $classids = {};
    my $autoid = 0;

    foreach my $class (keys %$classes)
    {
	  next if $classes->{$class}{abstract};
	  next unless defined $classes->{$class}{id};
	  my $id = $classids->{$class} = $classes->{$class}{id};
	  $autoid = $id if $id > $autoid;
    }

    foreach my $class (keys %$classes)
    {
	  next if $classes->{$class}{abstract};
	  next if defined $classes->{$class}{id};
	  $classids->{$class} = ++$autoid;
    }

    return $classids;
}

1;
