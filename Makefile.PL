use ExtUtils::MakeMaker;

use DBI;
use Set::Object;

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

if (-e 't/CONFIG')
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

   $cs = "DBI:$cs" unless $cs =~ /^DBI\:/i;
   
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
}

{

    require 't/Springfield.pm';
    print "\nNow I will attempt to connect and prepare the database...";

    local $/;
    open CONFIG, 't/CONFIG' or die "Cannot open 't/CONFIG', reason: $!";

    my ($cs, $user, $passwd) = split "\n", <CONFIG>;

    if (my $dbh = DBI->connect( $cs, $user, $passwd ))
    {
	print "it worked!\nSay 'make test' to run the test suite.\n\n";
	my $rs = $Springfield::schema->relational_schema();
	do { local $dbh->{PrintError}; $rs->retreat($dbh); };
	$rs->deploy($dbh);
	
	$dbh->disconnect;
    }
}

NOTESTS:

WriteMakefile(
    'NAME'	=> 'Tangram',
    'VERSION_FROM' => 'Tangram.pm', # finds $VERSION
);
