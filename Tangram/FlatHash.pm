# (c) Sound Object Logic 2000-2001

use strict;

package Tangram::FlatHash::Expr;

sub new
{
	my $pkg = shift;
	bless [ @_ ], $pkg;
}

sub includes
{
	my ($self, $item) = @_;
	my ($coll, $memdef) = @$self;

	$item = Tangram::String::quote($item)
		if $memdef->{string_type};

	my $coll_tid = 't' . $coll->root_table;
	my $data_tid = 't' . Tangram::Alias->new;

	return Tangram::Filter->new
		(
		 expr => "$data_tid.coll = $coll_tid.id AND $data_tid.v = $item",
		 tight => 100,      
		 objects => Set::Object->new($coll, Tangram::Table->new($memdef->{table}, $data_tid) ),
		 data_tid => $data_tid # for prefetch
		);
}

sub exists
{
	my ($self, $item) = @_;
	my ($coll, $memdef) = @$self;

	$item = Tangram::String::quote($item)
		if $memdef->{string_type};

	my $coll_tid = 't' . $coll->root_table;

	return Tangram::Filter->new
		(
		 expr => "EXISTS (SELECT * FROM $memdef->{table} WHERE coll = $coll_tid.id AND v = $item)",
		 objects => Set::Object->new($coll),
		);
}

package Tangram::FlatHash;

use base qw( Tangram::AbstractHash );
use Tangram::AbstractHash;

$Tangram::Schema::TYPES{flat_hash} = Tangram::FlatHash->new;

sub reschema
{
    my ($self, $members, $class) = @_;
    
    for my $field (keys %$members)
    {
		my $def = $members->{$field};
		my $refdef = ref($def);

		unless ($refdef)
		{
			# not a reference: field => field
			$def = $members->{$field} = { type => 'string',
						      key_type => 'string'
						    };
		}

		$def->{table} ||= $class . "_$field";
		$def->{type} ||= 'string';
		$def->{string_type} = $def->{type} eq 'string';
		$def->{sql} ||= $def->{string_type} ? 'VARCHAR(255)' : uc($def->{type});
		$def->{key_type} ||= 'string';
		$def->{key_string_type} = $def->{key_type} eq 'string';
		$def->{key_sql} ||= $def->{key_string_type} ? 'VARCHAR(255)' : uc($def->{key_type});
    }

    return keys %$members;
}

sub demand
{
	my ($self, $def, $storage, $obj, $member, $class) = @_;

	print $Tangram::TRACE "loading $member\n" if $Tangram::TRACE;
   
	my %coll;

	if (my $prefetch = $storage->{PREFETCH}{$class}{$member}{$storage->id($obj)})
	{
		%coll = %$prefetch;
	}
	else
	{
		my $id = $storage->id($obj);

		my $sth = $storage->sql_prepare(
            "SELECT a.k, a.v FROM $def->{table} a WHERE coll = $id", $storage->{db});

		$sth->execute();
		
		for my $row (@{ $sth->fetchall_arrayref() })
		{
			my ($k, $v) = @$row;
			$coll{$k} = $v;
		}
	}

	$self->set_load_state($storage, $obj, $member, { %coll } );

	return \%coll;
}

sub get_exporter
  {
	my ($self, $field, $def, $context) = @_;

	return sub {
	  my ($obj, $context) = @_;
	  $self->defered_save($context->{storage}, $obj, $field, $def);
	  ();
	}
  }

sub hash_diff {
  my ($first,$second,$differ) = @_;
  my (@common,@changed,@only_in_first,@only_in_second);
  foreach (keys %$first) {
    if (exists $second->{$_}) {
      if ($differ->($first->{$_},$second->{$_})) {
	push @changed, $_;
      }
      else {
	push @common, $_;
      }
    }
    else {
      push @only_in_first, $_;
    }
  }

  foreach (keys %$second) {
    push @only_in_second, $_ unless exists $first->{$_};
  }

  (\@common,\@changed,\@only_in_first,\@only_in_second);
}

sub defered_save
  {
	use integer;
	
	my ($self, $storage, $obj, $field, $def) = @_;
	
	return if tied $obj->{$field}; # collection has not been loaded, thus not modified

	my $coll_id = $storage->id($obj);
	
	my ($ne, $modify, $add, $remove) =
	  $self->get_save_closures($storage, $obj, $def, $coll_id);
	
	my $new_state = $obj->{$field} || {};
	my $old_state = $self->get_load_state($storage, $obj, $field) || {};
	
	my ($common, $changed, $to_add, $to_remove) = hash_diff($new_state, $old_state, $ne);
	
	for my $key (@$changed)
	  {
		$modify->($key, $new_state->{$key}, $old_state->{$key});
	  }
	
	for my $key (@$to_add)
	  {
		$add->($key, $new_state->{$key});
	  }
	
	for my $key (@$to_remove)
	  {
		$remove->($key);
	  }
	
	$self->set_load_state($storage, $obj, $field, { %$new_state } );	
	
	$storage->tx_on_rollback(
							 sub { $self->set_load_state($storage, $obj, $field, $old_state) } );
  }

my $no_ref = 'illegal reference in flat hash';

sub get_save_closures
{
	my ($self, $storage, $obj, $def, $id) = @_;

	my $table = $def->{table};

	my ($ne, $quote, $key_quote);

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

	if ($def->{key_string_type})
	{
		$key_quote = sub { $storage->{db}->quote(shift()) };
	}
	else {
		$key_quote = sub { shift() };
	}
	
	my $modify = sub
	{
		my ($k, $v) = @_;
		die $no_ref if (ref($v) or ref($k));
		$v = $quote->($v);
		$k = $key_quote->($k);
		$storage->sql_do("UPDATE $table SET v = $v WHERE coll = $id AND k = $k");
	};

	my $add = sub
	{
		my ($k, $v) = @_;
		die $no_ref if (ref($v) or ref($k));
		$v = $quote->($v);
		$k = $key_quote->($k);
		$storage->sql_do("INSERT INTO $table (coll, k, v) VALUES ($id, $k, $v)");
	};

	my $remove = sub
	{
		my ($k) = @_;
		die $no_ref if ref($k);
		$k = $key_quote->($k);
		$storage->sql_do("DELETE FROM $table WHERE coll = $id AND k = $k");
	};

	return ($ne, $modify, $add, $remove);
}

sub erase
{
	my ($self, $storage, $obj, $members, $coll_id) = @_;

	foreach my $def (values %$members)
	{
		my $id = $storage->id($obj);
		$storage->sql_do("DELETE FROM $def->{table} WHERE coll = $id");
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
		 k => $member->{key_sql},
		 v => $member->{sql}
		};
    }
}

sub query_expr
{
	my ($self, $obj, $members, $tid) = @_;
	map { Tangram::FlatHash::Expr->new($obj, $_); } values %$members;
}

sub prefetch
{
	my ($self, $storage, $def, $coll, $class, $member, $filter) = @_;

	my $prefetch = $storage->{PREFETCH}{$class}{$member} ||= {};

	my $restrict = $filter ? ', ' . $filter->from() . ' WHERE ' . $filter->where() : '';

	my $sth = $storage->sql_prepare(
        "SELECT coll, k, v FROM $def->{table} $restrict", $storage->{db});
	$sth->execute();
		
	for my $row (@{ $sth->fetchall_arrayref() })
	{
		my ($id, $k, $v) = @$row;
		$prefetch->{$id}{$k} = $v;
	}

	return $prefetch;
}

1;
