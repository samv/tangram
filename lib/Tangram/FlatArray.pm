# (c) Sound Object Logic 2000-2001

use strict;

package Tangram::FlatArray::Expr;

sub new
{
	my $pkg = shift;
	bless [ @_ ], $pkg;
}

sub quote
  {
	my $item = shift or return 'NULL';
	$item =~ s/'/''/g;	# 'emacs ;
	$item = "'$item'";
	return $item;
  }

sub includes
{
	my ($self, $item) = @_;
	my ($coll, $memdef) = @$self;

	my $schema = $coll->{storage}{schema};

	$item = quote($item)
	  if $memdef->{string_type};

	my $coll_tid = 't' . $coll->root_table;
	my $data_tid = 't' . Tangram::Alias->new;

	return Tangram::Filter->new
		(
		 expr => "$data_tid.coll = $coll_tid.$schema->{sql}{id_col} AND $data_tid.v = $item",
		 tight => 100,      
		 objects => Set::Object->new($coll, Tangram::Table->new($memdef->{table}, $data_tid) ),
		 data_tid => $data_tid # for prefetch
		);
}

sub exists
{
	my ($self, $item) = @_;
	my ($coll, $memdef) = @$self;

	my $schema = $coll->{storage}{schema};

	$item = quote($item)
	  if $memdef->{string_type};

	my $coll_tid = 't' . $coll->root_table;

	return Tangram::Filter->new
		(
		 expr => "EXISTS (SELECT * FROM $memdef->{table} WHERE coll = $coll_tid.$schema->{sql}{id_col} AND v = $item)",
		 objects => Set::Object->new($coll),
		);
}

package Tangram::FlatArray;

use vars qw(@ISA);
 @ISA = qw( Tangram::AbstractArray );
use Tangram::AbstractArray;

$Tangram::Schema::TYPES{flat_array} = Tangram::FlatArray->new;

sub reschema
{
    my ($self, $members, $class, $schema) = @_;
    
    for my $field (keys %$members)
    {
		my $def = $members->{$field};
		my $refdef = ref($def);

		unless ($refdef)
		{
			# not a reference: field => field
			$def = $members->{$field} = { type => 'string' };
		}

		$def->{table} ||= $schema->{normalize}->($class . "_" .$schema->{normalize}->($field, "fieldname"), 'tablename');
		$def->{type} ||= 'string';
		$def->{string_type} = $def->{type} eq 'string';
		$def->{sql} ||= $def->{string_type} ? 'VARCHAR(255)' : uc($def->{type});
    }

    return keys %$members;
}

sub demand
{
	my ($self, $def, $storage, $obj, $member, $class) = @_;

	print $Tangram::TRACE "loading $member\n" if $Tangram::TRACE;
   
	my @coll;
	my $id = $storage->export_object($obj);

	if (my $prefetch = $storage->{PREFETCH}{$class}{$member}{$id})
	{
		@coll = @$prefetch;
	}
	else
	{
		my $sth = $storage->sql_prepare(
            "SELECT\n    a.i,\n    a.v\nFROM\n    $def->{table} a\nWHERE\n    coll = $id", $storage->{db});

		$sth->execute();
		
		for my $row (@{ $sth->fetchall_arrayref() })
		{
			my ($i, $v) = @$row;
			$coll[$i] = $v;
		}
	}

	$self->set_load_state($storage, $obj, $member, [ @coll ] );

	return \@coll;
}

sub get_exporter
  {
	my ($self, $context) = @_;

	return sub {
	  my ($obj, $context) = @_;
	  $self->defered_save($context->{storage}, $obj, $self->{name}, $self);
	  ();
	}
  }

my $no_ref = 'illegal reference in flat array';

sub get_save_closures
{
	my ($self, $storage, $obj, $def, $id) = @_;

	my $table = $def->{table};

	my ($ne, $quote);

	if ($def->{string_type})
	{
		$ne = sub { my ($a, $b) = @_; defined($a) != defined($b) || $a ne $b };
		$quote = sub { $storage->{db}->quote(shift()) };
	}
	else
	{
		$ne = sub { my ($a, $b) = @_; defined($a) != defined($b) || $a != $b };
		$quote = sub { shift() };
	}

	my $eid = $storage->{export_id}->($id);

	my $modify = sub
	{
		my ($i, $v) = @_;
		die $no_ref if ref($v);
		$v = $quote->($v);
		$storage->sql_do("UPDATE\n    $table\nSET\n    v = $v\nWHERE\n    coll = $eid    AND\n    i = $i");
	};

	my $add = sub
	{
		my ($i, $v) = @_;
		die $no_ref if ref($v);
		$v = $quote->($v);
		$storage->sql_do("INSERT INTO $table (coll, i, v)\n    VALUES ($eid, $i, $v)");
	};

	my $remove = sub
	{
		my ($new_size) = @_;
		$storage->sql_do("DELETE FROM\n    $table\nWHERE\n    coll = $eid AND\n    i >= $new_size");
	};

	return ($ne, $modify, $add, $remove);
}

sub erase
{
	my ($self, $storage, $obj, $members, $coll_id) = @_;

	$coll_id = $storage->{export_id}->($coll_id);

	foreach my $def (values %$members)
	{
		$storage->sql_do("DELETE FROM\n    $def->{table}\nWHERE\n    coll = $coll_id");
	}
}

sub coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
		$tables->{ $member->{table} }{COLS} =
		{
		 coll => $schema->{sql}{id},
		 i => 'INT',
		 v => $member->{sql}
		};
    }
}

sub query_expr
{
	my ($self, $obj, $members, $tid) = @_;
	map { Tangram::FlatArray::Expr->new($obj, $_); } values %$members;
}

sub remote_expr
{
	my ($self, $obj, $tid) = @_;
	Tangram::FlatArray::Expr->new($obj, $self);
}

sub prefetch
{
	my ($self, $storage, $def, $coll, $class, $member, $filter) = @_;

	my $prefetch = $storage->{PREFETCH}{$class}{$member} ||= {};

	my $restrict = $filter ? ",\n" . $filter->from() . "\nWHERE\n    " . $filter->where() : '';

	my $sth = $storage->sql_prepare(
        "SELECT\n    coll,\n    i,\n    v\nFROM\n    $def->{table} $restrict", $storage->{db});
	$sth->execute();
		
	for my $row (@{ $sth->fetchall_arrayref() })
	{
		my ($id, $i, $v) = @$row;
		$prefetch->{$id}[$i] = $v;
	}

	# use Data::Dumper;	print STDERR Dumper $storage->{PREFETCH}, "\n";

	return $prefetch;
}

1;
