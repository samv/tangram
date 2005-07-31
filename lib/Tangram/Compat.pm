
# package for compatilibity with older Tangram APIs.

# first major change: Tangram::Scalar => Tangram::Type::Scalar, etc

package Tangram::Compat;
use Tangram::Compat::Stub;

use constant REMAPPED =>
    qw( Tangram::Scalar   Tangram::Type::Scalar
	Tangram::RawDate  Tangram::Type::RawDate
      );

use strict 'vars', 'subs';
use Carp qw(cluck confess croak carp);

sub DEBUG() { 0 }
sub debug_out { print STDERR __PACKAGE__.": @_\n" }

our $stub;
BEGIN { $stub = $INC{'Tangram/Compat/Stub.pm'} };

# this method is called when you "use" something.  This is a "Chain of
# Command Patte<ETOOMUCHBS>

sub INC {
    my $self = shift;
    my $fn = shift;

    (my $pkg = $fn) =~ s{/}{::}g;
    $pkg =~ s{.pm$}{};

    (DEBUG) && debug_out "saw include for $pkg";

    if ($self->{map}->{$pkg}) {
	$self->setup($pkg);
	open DEVNULL, "<$stub" or die $!;
	return \*DEVNULL;
    }
    else {
	return undef;
    }
}

sub setup {
    debug_out("setup(@_)") if (DEBUG);
    my $self = shift;
    my $pkg = shift or confess ("no pkg!");
    undef &{"${pkg}::AUTOLOAD"};
    my $target = delete $self->{map}{$pkg};
    confess "no target package" unless $target;

    carp "deprecated package $pkg used by ".caller().", auto-loading $target";

    debug_out("using $target") if (DEBUG);
    eval "use $target"; die $@ if $@;
    my $eval = "package $pkg; \@ISA = qw($target)";
    debug_out("creating $pkg with: $eval") if (DEBUG);
    eval $eval; die $@ if $@;
    #@{"${pkg}::ISA"} = $target;
    if ( @_ ) {
	my $method = shift;
	$method =~ s{.*::}{};
	@_ = @{(shift)};
	my $code = $target->can($method)
	    or do {
		debug_out("pkg is $pkg, its ISA is ".join(",",@{"${pkg}::ISA"})) if (DEBUG);
		croak "$target->can't($method)";
	    };
	goto $code;
    }
}

our $AUTOLOAD;

sub new {
    my $inv = shift;
    my $self = bless { map => { @_ },
		     }, (ref $inv||$inv);
    for my $pkg ( keys %{$self->{map}} ) {
	debug_out "setting up $pkg => $self->{map}{$pkg}" if DEBUG;

	*{"${pkg}::AUTOLOAD"} = sub {
	    return if $AUTOLOAD =~ /::DESTROY$/;
	    debug_out "pkg is $pkg, AUTOLOAD is $AUTOLOAD" if DEBUG;
	    my $stack = [ @_ ];
	    @_ = ($self, $pkg, $AUTOLOAD, $stack);
	    goto &setup;
	};
    }
}

BEGIN {
    unshift @INC, __PACKAGE__ ->new( REMAPPED );
}

1;
