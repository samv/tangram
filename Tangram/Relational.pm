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

sub _with_handle {
  my $method = shift;
  my $schema = shift;

  if (@_) {
	my $arg = shift;

	if (ref $arg) {
	  Tangram::Relational::Engine->new($schema)->$method($arg)
	} else {
	  my $dbh = DBI->connect($arg, @_);
	  eval { Tangram::Relational::Engine->new($schema)->$method($dbh) };
	  $dbh->disconnect();
  
	  die $@ if $@;
	}
  } else {
	Tangram::Relational::Engine->new($schema)->$method();
  }
}

sub deploy
  {
	shift;
	_with_handle('deploy', @_);
  }

sub retreat
  {
	shift;
	_with_handle('retreat', @_);
  }

1;
