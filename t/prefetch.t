#!/usr/bin/perl -ws

use strict;

# command line parameters - see perlrun (-s switch)
use vars qw($debug);

use Test::More tests => 55;

use lib "t";
use Springfield qw(%id stdpop leaked @kids);

my $tests = {(
	      "IntrArray" => [ 1, "ia_children", "NaturalPerson", ],
	      "Array"     => [ 0, "children",    "NaturalPerson", ],
	      "Hash"      => [ 0, "h_opinions",  "Opinion",       ],
	      #"IntrHash"  => [ 1, "ih_opinions", "Opinion",       ],
	      "Set"       => [ 0, "s_children",  "NaturalPerson", ],
	      "IntrSet"   => [ 1, "is_children", "NaturalPerson", ],
	     )};
while (my ($test_name, $data) = each %$tests) {
    my ($intrusive, $children, $class) = @{ $data };
    test_prefetch($test_name, $intrusive, $children, $class);
}

sub test_prefetch {
    my ($test_name, $intrusive, $children, $class) = (@_);

    stdpop($children);

    for my $do_prefetch (0..1) {

	# Check that the test is valid:
	{
	    my $storage = Springfield::connect;

	    #local($Tangram::TRACE) = \*STDOUT;
	    my $Homer = $storage->load( $id{Homer} );

	    my ($r_parent, $r_child) = $storage->remote( "NaturalPerson",
							 $class );

	    my $filter = ($r_parent->{$children}->includes($r_child)
			  & ($r_parent == $Homer));

	    my @parents  = $storage->select( $r_parent, $filter );
	    my @children = $storage->select( $r_child,  $filter );
	    @children = sort {
		my $ix_a;
		my $ix_b;
		my $count = 0;
		for (@kids) {
		    $ix_a = $count if ($a->{firstName} eq $_);
		    $ix_b = $count if ($b->{firstName} eq $_);
		    $count++;
		}
		$ix_a <=> $ix_b;
	    } @children if ($children =~ /children/);
	    ok(@children, "$test_name - Got some children back");

	    $storage->prefetch( 'NaturalPerson',
				$children,
				$filter ) if $do_prefetch;

	    $storage->{db}->disconnect(); # hyuk yuk yuk

	    local($SIG{__WARN__}) = sub { };
	    my @new_children;
	    my $sort = sub {
		(exists $a->{firstName} ?
		 ( $a->{firstName} cmp $b->{firstName} )
		 : ( $a->{statement} cmp $b->{statement} ) );
	    };
	    eval {
		if ($children =~ m/s_children/) {
		    @new_children = sort { $sort->() }
			$Homer->{$children}->members;
		    @children = sort { $sort->() } @children;

		} elsif ($children =~ m/children/) {
		    @new_children = @{ $Homer->{$children} };
		} else {
		    @new_children = sort { $sort->() }
					 values %{ $Homer->{$children} };
		    @children = sort { $sort->() } @children;
		}
	    };

	    if ($do_prefetch) {

		is($@, "", "$test_name - Didn't raise an exception w/prefetch");
		#local ($,)=" ";
		#print map { ref $_ ? $_->{firstName} : $_ }
		    #"Sent:", @children,
		    #"\nGot: ", @new_children, "\n";
		is_deeply(\@new_children, \@children, "$test_name - got back what we put in");
	    } else {
		isnt($@, "", "$test_name - Raises an exception w/o prefetch");
		isnt(@new_children, @children,
		     "$test_name - didn't get back what we put in");
	    }

	    $storage->disconnect();

	}
	is(leaked, 0, "Leaktest");
    }
}

$a = $b = "globals";
