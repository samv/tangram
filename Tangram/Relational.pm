package Tangram::Relational;

sub connect
  {
	my $self = shift;
	return Tangram::Storage->connect( @_ );
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
