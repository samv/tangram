
package TestNeeds;

use Data::Dumper;

sub import {
    my $package = shift;
    my $caller = caller();

    my @missing;

    while ( my $package = shift ) {
	my $import = "";
	if ( @_ and ref $_[0] ) {
	    local($Data::Dumper::Purity) = 1;
	    $import = Data::Dumper::Dumper(${(shift)});
	} elsif ( @_ and $_[0] =~ m/^[0-9\.\-][\w\-\.]*$/) {
	    $import = shift;
	}
	eval "package $caller; use $package $import;";
	push @missing, $package, $import, $@ if $@;
    }

    if ( @missing ) {
	print("0..0 # Skipping test suite due to missing/broken "
	      ."dependancies\n");
	while ( my ($pkg, $args, $err) = splice @missing, 0, 3 ) {
	    print STDERR ("ERROR - pre-requisite $pkg "
			  .($args ? "$args " : "")
			  ."failed to load ($err)\n");
	}
	exit(1);
    }

}

1;
