# (c) Sound Object Logic 2000-2001

use strict;

use Tangram::Type;

package Tangram::Scalar;

use base qw( Tangram::Type );

sub reschema
{
    my ($self, $members, $class) = @_;

    if (ref($members) eq 'ARRAY')
    {
		# short form
		# transform into hash: { fieldname => { col => fieldname }, ... }
		$members = $_[1] = map { $_ => { col => $_ } } @$members;
    }
    
    for my $field (keys %$members)
    {
		my $def = $members->{$field};

		unless (ref($def))
		{
			# not a reference: field => field
			$def = $members->{$field} = { col => $def || $field };
		}

		$self->field_reschema($field, $def);
    }

    return keys %$members;
}

sub field_reschema
  {
	my ($self, $field, $def) = @_;
	$def->{col} ||= $field;
  }

sub get_exporter
  {
	my ($self) = @_;
	return if $self->{automatic};
	my $field = $self->{name};
	return "exists \$obj->{$field} ? \$obj->{$field} : undef";
  }

sub get_importer
  {
	my ($self) = @_;
	return "\$obj->{$self->{name}} = shift \@\$row";
  }

sub query_expr
{
    my ($self, $obj, $memdefs, $tid, $storage) = @_;
    return map { $storage->expr($self, "t$tid.$memdefs->{$_}{col}", $obj) } keys %$memdefs;
}

sub cols
{
    my ($self, $members) = @_;
    map { $_->{col } } values %$members;
}

sub get_export_cols
{
  return shift->{col};
}

sub get_import_cols
{
    my ($self, $context) = @_;
	return $self->{col};
}

sub read
{
    my ($self, $row, $obj, $members) = @_;
    @$obj{keys %$members} = splice @$row, 0, keys %$members;
}

sub literal
{
    my ($self, $lit) = @_;
    return $lit;
}

sub content
{
    shift;
    shift;
}

package Tangram::Number;

use base qw( Tangram::Scalar );

sub save
{
    my ($self, $cols, $vals, $obj, $members) = @_;

    foreach my $member (keys %$members)
    {
		my $memdef = $members->{$member};

		next if $memdef->{automatic};

		push @$cols, $memdef->{col};
		push @$vals, exists($obj->{$member}) && defined ($obj->{$member})
			? $obj->{$member} : 'NULL';
    }
}

sub get_export_cols
{
    my ($self) = @_;
    return exists $self->{automatic} ? () : ($self->{col});
}

package Tangram::Integer;

use base qw( Tangram::Number );
$Tangram::Schema::TYPES{int} = Tangram::Integer->new;

package Tangram::Real;

use base qw( Tangram::Number );

$Tangram::Schema::TYPES{real} = Tangram::Real->new;

package Tangram::String;

use base qw( Tangram::Scalar );

$Tangram::Schema::TYPES{string} = Tangram::String->new;

sub quote
{
	my $val = shift;
	return 'NULL' unless $val;
	$val =~ s/'/''/g;	# 'emacs
	return "'$val'";
}

sub save
  {
    my ($self, $cols, $vals, $obj, $members, $storage) = @_;
    
    my $dbh = $storage->{db};
    
    foreach my $member (keys %$members)
      {
	my $memdef = $members->{$member};
	
	next if $memdef->{automatic};
	
	push @$cols, $memdef->{col};
	
	if (exists $obj->{$member})
	  {
	    push @$vals, $dbh->quote($obj->{$member});
	  }
	else
	  {
	    push @$vals, 'NULL';
	  }
      }
  }

sub literal
  {
    my ($self, $lit, $storage) = @_;
    return $storage->{db}->quote($lit);
}

1;



