use strict;
use DBI;

system 'mysqladmin -f -u root drop jllallocid 2>/dev/null';
system 'mysqladmin -u root create jllallocid';

my @cp = qw( dbi:mysql:database=jllallocid root );

my $test = shift or die;
my $p = shift || 10;
my $n = shift || 10000;

sub autoincrement_deploy {
  my $dbh = DBI->connect(@cp);
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

sub autoincrement {
  my $dbh = DBI->connect(@cp);
  my $insert_person = $dbh->prepare('INSERT INTO Person(type) VALUES (?)');
  my $get_id = $dbh->prepare('SELECT LAST_INSERT_ID()');
  my $insert_np = $dbh->prepare('INSERT INTO NaturalPerson(id, name) VALUES (?, ?)');

  for my $i (1..$n) {
	$insert_person->execute(1);
	$get_id->execute();
	my $id = $get_id->fetchall_arrayref()->[0][0];
	print "$id\n";
	$insert_np->execute($id, $i);
  }

  $dbh->disconnect();
}

sub manual_deploy {
  my $dbh = DBI->connect(@cp);
  $dbh->do(q{
	CREATE TABLE Tangram
	  (
	   id INTEGER NOT NULL AUTO_INCREMENT,
	   PRIMARY KEY( id ),
	   type INTEGER NOT NULL
	  )
	});
  
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

sub manual {
  my $dbh = DBI->connect(@cp);
  my $insert_person = $dbh->prepare('INSERT INTO Person(type) VALUES (?)');
  my $get_id = $dbh->prepare('SELECT LAST_INSERT_ID()');
  my $insert_np = $dbh->prepare('INSERT INTO NaturalPerson(id, name) VALUES (?, ?)');

  for my $i (1..$n) {
	$insert_person->execute(1);
	$get_id->execute();
	my $id = $get_id->fetchall_arrayref()->[0][0];
	#print "$id\n";
	$insert_np->execute($id, $i);
  }

  $dbh->disconnect();
}

sub run {
  my $fun = shift;
  for (1..$p) {
	if (fork) {
	  $fun->();
	  exit;
	}
  }

  print "waiting for children\n";
  wait;
}

use Benchmark;
no strict;
print "$test $p $n\n";
&{"${test}_deploy"};
timethis 1, "run(\\&$test)";
timethis 1, sub { run(\&autoincrement) };


