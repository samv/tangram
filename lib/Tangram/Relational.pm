# (c) Sound Object Logic 2000-2001

use Tangram::Relational::Engine;

package Tangram::Relational;

sub new { bless { }, shift }

sub connect
  {
	my ($pkg, $schema, $cs, $user, $pw, $opt) = @_;
	$opt ||= {};
	$opt->{driver} = $pkg->new();
	my $storage
	    = Tangram::Storage->connect( $schema, $cs, $user, $pw, $opt );
  }

sub schema
  {
	my $self = shift;
	return Tangram::Schema->new( @_ );
  }

sub _with_handle {
    my $self = shift;
  my $method = shift;
  my $schema = shift;

  if (@_) {
	my $arg = shift;

	if (ref $arg) {
	  Tangram::Relational::Engine->new($schema, driver => $self)->$method($arg)
	} else {
	  my $dbh = DBI->connect($arg, @_);
	  eval { Tangram::Relational::Engine->new($schema, driver => $self)->$method($dbh) };
	  $dbh->disconnect();
  
	  die $@ if $@;
	}
  } else {
	Tangram::Relational::Engine->new($schema, driver => $self)->$method();
  }
}

sub deploy
  {
      my $self = (shift) || __PACKAGE__;
      $self->_with_handle('deploy', @_);
  }

sub retreat
  {
      my $self = (shift) || __PACKAGE__;
      $self->_with_handle('retreat', @_);
  }

# handle virtual SQL types.  Isn't SQL silly?
our ($sql_t_qr, @sql_t);
BEGIN {
    @sql_t =
	(
	 'VARCHAR'     => 'varchar',       # variable width
	 'CHAR'        => 'char',          # fixed width
	 'BLOB'        => 'blob',          # generic, large data store
	 'DATE|TIME|DATETIME|TIMESTAMP'
	               => 'date',
	 'BOOL'        => 'bool',
	 'INT|SHORTINT|TINYINT|LONGINT|MEDIUMINT|SMALLINT'
                       => 'integer',
	 'DECIMAL|NUMERIC|FLOAT|REAL|DOUBLE|SINGLE|EXTENDED'
	               => 'number',
	 'ENUM|SET'    => 'special',
	 ''            => 'general',
	);

    # compile the types to a single regexp.
    {
	my $c = 0;
	$sql_t_qr = "^(?:".join("|", map { "($_)" } grep {(++$c)&1}
				@sql_t).")";

	$sql_t_qr = qr/$sql_t_qr/i;
    }
}

sub type {
    my $self = shift if ref $_[0] or UNIVERSAL::isa($_[0], __PACKAGE__);
    $self ||= __PACKAGE__;
    my $type = shift;

    my @x = ($type =~ m{$sql_t_qr});

    my $c = 1;
    $c+=2 while not defined shift @x;

    my $func = $sql_t[$c];
    return $self->$func($type);

}

# function to convert to an RDBMS date
sub dbms_date {
    my $self = shift if ref $_[0] or UNIVERSAL::isa($_[0], __PACKAGE__);

    my $iso8660_date = Date::Manip::ParseDate(shift);

    return $iso8660_date;
}

# default mappings are no-ops
BEGIN {
    no strict 'refs';
    my $c = 0;
    *{$_} = sub { shift if UNIVERSAL::isa($_[0], __PACKAGE__); shift; }
	foreach grep {($c++)&1} @sql_t;
}

1;
