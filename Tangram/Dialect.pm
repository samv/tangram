use strict;

package Tangram::Dialect;

my @dialects;

sub register
  {
    push @dialects, shift;
  }

sub rate_connect_string
  {
    return 0;
  }

Tangram::Dialect->register();

sub guess
  {
    my ($class, $cs) = @_;
    
    my @rated = sort { $b->[1] <=> $a->[1] }
    grep { $_->[1] }
    map { [ $_, $_->rate_connect_string($cs) ] }
    @dialects;
    
    return @rated if wantarray;
    
    die "cannot select dialect, more than one have equal rating"
      if @rated > 1 && $rated[0][1] == $rated[1][1];
    
    return @rated ? $rated[0][0]->new : undef();
  }

sub new
  {
    my $class = shift;
    return bless { @_ }, $class;
  }

sub expr
  {
    my $self = shift;
    return shift->expr( @_ );
  }

sub make_id
  {
    shift;
    return shift->std_make_id(@_);
  }

sub tx_start
  {
    shift;
    shift->std_tx_start(@_);
  }

sub tx_commit
  {
    shift;
    shift->std_tx_commit(@_);
  }

sub tx_rollback
  {
    shift;
    shift->std_tx_rollback(@_);
  }

1;
