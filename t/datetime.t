use strict;
use lib 't';
use Springfield;

# $Tangram::TRACE = \*STDOUT;   

Springfield::begin_tests(3);

my %ids;

{
	my $storage = Springfield::connect_empty;
   
	my $jll = NaturalPerson->new
		(
		 firstName => 'Jean-Louis',
		 birthDate => '1963-8-13',
		 birthTime => '11:34:17',
		 birth => '1963-8-13 11:34:17',
  		);
								   
	$ids{jll} = $storage->insert($jll);
   
	my $chloe = NaturalPerson->new
		(
		 firstName => 'Chloé',
		 birth => '1993-7-28 13:10:00',
  		);
								   
   $ids{chloe} = $storage->insert($chloe);

   $storage->disconnect;
}

Springfield::leaktest;

{
	my $storage = Springfield::connect;
   
	my $jll = $storage->load( $ids{jll} );
	
	if (0)
	{
		Springfield::test($jll->{birthTime} =~ /11/
						  && $jll->{birthTime} =~ /34/
						  && $jll->{birthTime} =~ /17/
						 );

		Springfield::test($jll->{birthDate} =~ /1963/
						  && $jll->{birthDate} =~ /13/
						  && $jll->{birthDate} =~ /8/
						 );
	}

	my $rp = $storage->remote(qw( NaturalPerson ));
	my @results = $storage->select( $rp, $rp->{birth} > '1990-1-1' );

	Springfield::test( @results == 1
					   && $storage->id( $results[0] ) == $ids{chloe} );

# 	if (optional_tests('epoch; no Time::Local',
# 					   eval { require 'Time::Local' }, 1)) {

# 		Springfield::test($jll->{birthDate} =~ /1963/
# 						  && $jll->{birthDate} =~ /13/
# 						  && $jll->{birthDate} =~ /8/
# 						 );
# 	}

	$storage->disconnect;
}

Springfield::leaktest;

1;
