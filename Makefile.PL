use ExtUtils::MakeMaker;

eval {use DBI;};
if($@) {
  warn "Couldn't load DBI... Skipping tests.";
  goto NOTESTS;
}

eval {use Set::Object;};
if($@) {
  warn "Couldn't load Set::Object... Skipping tests.";
  goto NOTESTS;
}

use lib '.';
use Tangram::Deploy;

sub yes
{
    print ' (Y/n) ';
    return <STDIN> =~ /^(Y|)$/i;
}

print q{Do you plan to run the regression tests?
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

   print "\n1) DBI connect string (you can omit the \'DBI:\' part): ";
   my $cs = <STDIN>;
   chop $cs;

   $cs = "dbi:$cs" unless $cs =~ /^dbi\:/i;
   
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
   print CONFIG "$cs\n$user\n$passwd\n";
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
    'VERSION_FROM' => 'Tangram.pm', # finds $VERSION
    PREREQ_PM => { 'Set::Object' => 1.03, DBI => 0, "Test::More" => 0 }
);
