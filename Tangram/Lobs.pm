use strict;
# Added by Marian Kelc <marian.kelc@ruhr-uni-bochum.de>
# 14.3.2000
# Quoting Strings correctly for LOBS (TEXT OR BLOB Types)
use Tangram::Scalar;

package Tangram::Blob;
use base qw( Tangram::String );
$Tangram::Schema::TYPES{blob} = Tangram::Blob->new;

sub Tangram::Blob::coldefs {
    my ($self, $cols, $members, $schema)= @_;
    $self->_coldefs($cols, $members, 'BLOB', $schema);
}

sub save
{
    my ($self, $cols, $vals, $obj, $members, $db) = @_;
    
    foreach my $member (keys %$members) {
	push @$cols, $members->{$member}{col};
	
	if (exists($obj->{$member}) 
	    && defined( my $val = $obj->{$member} )) {
	    
	    if( $db->{db}->can('quote') ) {
		$val= $db->{db}->quote( $val );
		push @$vals, $val;
	    } else {
		$val =~ s/'/''/g; # 'emacs
		push @$vals, "'$val'";
	    }
	    
	    
	} else {
	    push @$vals, 'NULL';
	}
    }
}

package Tangram::Text;
use base qw( Tangram::Blob );
$Tangram::Schema::TYPES{text} = Tangram::Text->new;

sub Tangram::Text::coldefs {
    my ($self, $cols, $members, $schema)= @_;
    $self->_coldefs($cols, $members, 'TEXT', $schema);
}

1;
