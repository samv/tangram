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
		$_[1] = map { $_ => { col => $_ } } @$members;
		return @$members;
		die 'coverok';
    }
    
    for my $field (keys %$members)
    {
		my $def = $members->{$field};
		my $refdef = ref($def);

		unless ($refdef)
		{
			# not a reference: field => field
			$members->{$field} = { col => $def || $field };
			next;
		}

		die ref($self), ": $class\:\:$field: unexpected $refdef"
			unless $refdef eq 'HASH';
	
		$def->{col} ||= $field;
    }

    return keys %$members;
}

sub query_expr
{
    my ($self, $obj, $memdefs, $tid, $storage) = @_;
	my $dialect = $storage->{dialect};
    return map { $dialect->expr($self, "t$tid.$_", $obj) } keys %$memdefs;
}

sub cols
{
    my ($self, $members) = @_;
    map { $_->{col } } values %$members;
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
		push @$cols, $members->{$member}{col};
		push @$vals, exists($obj->{$member}) && defined ($obj->{$member})
			? $obj->{$member} : 'NULL';
    }
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
    my ($self, $cols, $vals, $obj, $members) = @_;

    foreach my $member (keys %$members)
    {
		push @$cols, $members->{$member}{col};
		push @$vals, quote(exists($obj->{$member}) && $obj->{$member});
	}
}

sub literal
{
    my ($self, $lit) = @_;
    return "'$lit'";
}

1;



