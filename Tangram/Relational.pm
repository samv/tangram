# (c) Sound Object Logic 2000-2001

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
	shift->deploy( @_ );
  }

sub retreat
  {
	shift; # class
	shift->retreat( @_ );
  }

1;
