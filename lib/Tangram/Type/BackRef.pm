
package Tangram::Type::BackRef;

use strict;

use vars qw(@ISA);
 @ISA = qw( Tangram::Scalar );

$Tangram::Schema::TYPES{backref} = __PACKAGE__->new;

sub get_export_cols
  {
	()
  }

sub get_exporter
  {
  }

sub get_importer
{
  my ($self, $context) = @_;
  my $field = $self->{name};

  return sub {
	my ($obj, $row, $context) = @_;

	my $rid = shift @$row;

	if ($rid) {
	  tie $obj->{$field}, 'Tangram::Lazy::BackRef', $context->{storage}, $context->{id}, $self->{name}, $rid, $self->{class}, $self->{field};
	} else {
	  $obj->{$field} = undef;
	}
  }
}

1;
