use strict;
use DBI;
use Benchmark;

my @cp = qw( dbi:mysql:database=jllallocid root );

my $p = shift or die;
my $n = shift or die;
my $class = shift;
my $lock = @ARGV ? shift : 1;

my $dbh;

package test;

sub new {
  bless [], shift;
}

sub run {
  my $self = shift;
  $dbh = DBI->connect(@cp);
  $self->prepare($dbh);

  for my $i (1..$n) {
	$dbh->do('SELECT GET_LOCK("tx", 10)') if $lock;
	$self->insert($dbh);
	$dbh->do('SELECT RELEASE_LOCK("tx")') if $lock;
  }

  $dbh->disconnect();
}

package autoinc;
use base 'test';

sub deploy {
  $dbh = DBI->connect(@cp);
  $dbh->do(q{
	CREATE TABLE Person
	  (
	   id INTEGER NOT NULL AUTO_INCREMENT,
	   PRIMARY KEY( id ),
	   type INTEGER NOT NULL
	  )
	});
  
  $dbh->do(q{
	CREATE TABLE NaturalPerson
	  (
	   id INTEGER NOT NULL,
	   PRIMARY KEY( id ),
	   name VARCHAR(255) NULL
	  )
	});

  $dbh->disconnect();
}

sub prepare {
  my ($self, $dbh) = @_;
  @$self = map { $dbh->prepare($_) } 'INSERT INTO Person(type) VALUES (?)', 'SELECT LAST_INSERT_ID()', 'INSERT INTO NaturalPerson(id, name) VALUES (?, ?)'
}

sub insert {
  my ($self) = @_;
  my ($insert_person, $get_id, $insert_np) = @$self;

  $insert_person->execute(1);
  $get_id->execute();
  my $id = $get_id->fetchall_arrayref()->[0][0];
  # print "$id\n";
  $insert_np->execute($id, $id);
}

package tangram;
use base 'test';

sub deploy {
  $dbh = DBI->connect(@cp);
  $dbh->do(q{
	CREATE TABLE Tangram
	  (
	   mark INTEGER NOT NULL
	  )
	});

  $dbh->do('INSERT INTO Tangram (mark) VALUES (0)');

  $dbh->do(q{
	CREATE TABLE Person
	  (
	   id INTEGER NOT NULL AUTO_INCREMENT,
	   PRIMARY KEY( id ),
	   type INTEGER NOT NULL
	  )
	});

  $dbh->do(q{
	CREATE TABLE NaturalPerson
	  (
	   id INTEGER NOT NULL,
	   PRIMARY KEY( id ),
	   name VARCHAR(255) NULL
	  )
	});

  $dbh->disconnect();
}

sub prepare {
  my ($self, $dbh) = @_;
  @$self = map { $dbh->prepare($_) }
	'UPDATE Tangram SET mark = LAST_INSERT_ID(mark + 1)',
	'SELECT LAST_INSERT_ID()',
	'INSERT INTO Person(type) VALUES (?)',
	'INSERT INTO NaturalPerson(id, name) VALUES (?, ?)';
}

sub insert {
  my $self = shift;
  my ($get_id_update, $get_id_select, $insert_person, $insert_np) = @$self;

  $insert_person->execute(1);
  $get_id_update->execute();
  $get_id_select->execute();
  my $id = $get_id_select->fetchall_arrayref()->[0][0];
  # print "$id\n";
  $insert_np->execute($id, $id);
}

package random;
use base 'test';

sub deploy {
  $dbh = DBI->connect(@cp);
  $dbh->do(q{
	CREATE TABLE Person
	  (
	   id INTEGER NOT NULL,
	   PRIMARY KEY( id ),
	   type INTEGER NOT NULL
	  )
	});

  $dbh->do(q{
	CREATE TABLE NaturalPerson
	  (
	   id INTEGER NOT NULL,
	   PRIMARY KEY( id ),
	   name VARCHAR(255) NULL
	  )
	});

  $dbh->disconnect();
}

sub prepare {
  my ($self, $dbh) = @_;
  @$self = map { $dbh->prepare($_) } 'INSERT INTO Person(id, type) VALUES (?, ?)', 'INSERT INTO NaturalPerson(id, name) VALUES (?, ?)';
}

sub insert {
  my $self = shift;
  my ($insert_person, $insert_np) = @$self;

  my $id;

  do {
	$id = int(rand() * 2_000_000_000);
  } until ($insert_person->execute($id, 1));

  #print "$id\n";
  $insert_np->execute($id, $id);
}

package empty;
use base 'test';

sub deploy {
}

sub prepare {
}

sub insert {
}

package main;

$| = 1;
printf '%-10s %5d %5d lock=%-3s : ', $class, $p, $n, ($lock ? 'yes' : 'no');

system 'mysqladmin -f -u root drop jllallocid 2>&1 >/dev/null';
system 'mysqladmin -u root create jllallocid 2>&1 >/dev/null';

my $test = $class->new();

$test->deploy();

my $time = timeit 1, sub {

  if ($p == 1) {
	$test->run;
  } else {
	for (1..$p) {
	  my $child = fork;
	  die 'cannot fork!' unless defined $child;
	  unless ($child) {
		$test->run;
		exit;
	  }
	}
  }

  #print "waiting for children\n";
  wait;
};

print timestr($time, 'noc'), "\n";
