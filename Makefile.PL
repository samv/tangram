use ExtUtils::MakeMaker;

my %required =
    (
     'Set::Object' => 1.04,
     "Test::More" => 0,
     "Date::Manip" => 0,
     "Scalar::Util" => 0,
     DBI => 0,
    );

my %required_soft = map{$_=>1} qw( Scalar::Util Date::Manip );
my %required_hard = map{$_=>1} qw( Set::Object  DBI );

my ($skip_tests, $bomb_out);
while (my ($module, $min_ver) = each %required) {

    eval "use $module".($min_ver ? " $min_ver" : "");
    if($@) {
	if (exists $required_hard{$module}) {
	    $bomb_out = 1;
	} elsif ( exists $required_soft{$module} ) {
	    print STDERR ("$module not found.  Some tests and/or "
			  ."functionality may be missing.\n");
	} else {
	    $skip_tests = 1;
	    print STDERR ("$module not found.  You will not be able "
			  ."to run the test suite.\n");
	}
    }
}

goto NOTESTS if $skip_tests or $bomb_out;

use lib '.';
use Tangram::Deploy;

sub yes
{
    print ' (Y/n) ';
    return <STDIN> =~ /^(Y(e(s)?)?|A(YE|II+!*))?\n?$/i;
}

sub yeah_no  # it's an antipodean thing
{
    print ' (N/y) ';
    return <STDIN> =~ /^(Y(e(s)?)?|A(YE|II+!*))\n?$/i;

}

print q{Do you plan to run the test suite?
(you will need to set up an *EMPTY* database)};

goto NOTESTS unless yes();

my $configured;

if ($ENV{TANGRAM_CONFIG})
{
   print qq{
You have set TANGRAM_CONFIG to $ENV{TANGRAM_CONFIG}.
Should I use it?};
   $configured = yes();
}

if (!$configured && -e 't/CONFIG')
{
   print q{
It looks like there is a 't/CONFIG' file already. It probably contains
connection information from a previous installation. Should I use it?};
   $configured = yes();
}

unless ($configured)
{
   print q{
Please give me the login and password for accessing the test database.
I must be able to create and drop tables in that database.};

   print "\n1) DBI connect string (omit the \'DBI:\' part): ";
   my $cs = <STDIN>;
   chop $cs;

   $cs = "dbi:$cs" unless $cs =~ /^dbi\:/i;

   my ($use_tx, $use_subsel, $table_type) = (1, 1);

   if ($cs =~ m/:mysql:/i) {
       print q{
You have selected the MySQL back-end.  Normally, subselects and
transactions are disabled for this database.  However, if you are
using MySQL-Max, or some other MySQL version with InnoDB support
compiled in and configured, you can enable the transaction support for
the test suite.  If you want to use it in your programs, you'll have
to set no_tx = 0 in the options to Tangram::Storage->connect() (or
hope that the auto-detection of the feature works), and table_type =
InnoDB in the Schema.  See Tangram::Schema and Tangram::Storage for
more information.

Alternatively, if you are running MySQL 4.1 or later, you can enable
sub-select tests.  It is not possible to use sub-selects with InnoDB
table types.

Use InnoDB tables};
       unless (yeah_no()) {
	   $use_tx = 0;

	   print "Use sub-selects";
	   unless (yeah_no()) {
	       $use_subsel = 0;
	   }
       } else {
	   $table_type = "InnoDB";
       }
   }
   
   print "2) Login: ";
   my $user = <STDIN>;
   chop $user;
   
   print "3) Password: ";
   my $passwd = <STDIN>;
   chop $passwd;

   print <<'MSG';

Thank you. I am going to save this information to 't/CONFIG'.
If you have given me sensitive information, make sure to destroy
the file when the tests have been completed.
MSG

   open CONFIG, '>t/CONFIG' or die "Cannot create 't/CONFIG', reason: $!";
   print CONFIG "$cs\n$user\n$passwd\ntx_do = $use_tx\nsubselects = $use_subsel\n".($table_type?"table_type = $table_type\n":"");
   close CONFIG;
}

{

    require 't/Springfield.pm';
    print "\nNow I will attempt to connect and prepare the database...";

    local $/;

    if (my $dbh = DBI->connect( $Springfield::cs, $Springfield::user, $Springfield::passwd ))
    {
		do
		{
		  local $dbh->{PrintError};
		  $Springfield::dialect->retreat($Springfield::schema, $dbh);
		};

		$Springfield::dialect->deploy($Springfield::schema, $dbh);
		$dbh->disconnect;
		print "it worked!\nSay 'make test' to run the test suite.\n\n";
    }
}

NOTESTS:

WriteMakefile(
    'NAME'	=> 'Tangram',
    'VERSION_FROM' => 'lib/Tangram.pm', # finds $VERSION
    PREREQ_PM => \%required,
);
