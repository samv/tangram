# (c) Sound Object Logic 2000-2001

use Tangram::Relational::Engine;

package Tangram::Relational;

sub connect
  {
	my $self = shift;
	return Tangram::Storage->connect( @_ );
  }

sub schema
  {
	my $self = shift;
	return Tangram::Schema->new( @_ );
  }

sub deploy
  {
	shift; # class
	my ($schema, $handle) = @_;
	Tangram::Relational::Engine->new($schema)->deploy($handle);
  }

sub retreat
  {
	shift; # class
	my ($schema, $handle) = @_;
	Tangram::Relational::Engine->new($schema)->retreat($handle);
  }

1;
