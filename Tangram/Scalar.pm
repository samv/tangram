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

sub query_expr
{
    my ($self, $obj, $memdefs, $tid, $storage) = @_;
    return map { $storage->expr($self, "t$tid.$memdefs->{$_}{col}", $obj) } keys %$memdefs;
}

sub remote_expr
{
    my ($self, $obj, $tid, $storage) = @_;
    $storage->expr($self, "t$tid.$self->{col}", $obj);
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

sub get_export_cols
{
  return shift->{col};
}

sub get_import_cols
{
    my ($self, $context) = @_;
	return $self->{col};
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

sub literal
  {
    my ($self, $lit, $storage) = @_;
    return $storage->{db}->quote($lit);
}

1;



