BEGIN {
    my @not_found;

    foreach my $module (qw(Class::Accessor::Assert Time::Piece)) {
	eval "use $module";
	if ( $@ ) {
	    push @not_found, $module;
	}
    }

    if ( @not_found ) {
	require 'Test/More.pm';
	Test::More->import(skip_all => "Test case requires extra modules: @not_found");
    }
}

1;
