use strict;

package Tangram::Dialect::Mysql;

use Tangram::Dialect;
use base qw( Tangram::Dialect );

sub rate_connect_string
  {
    my ($self, $cs) = @_;
    return +($cs =~ m/^dbi:mysql:/i);
  }

sub tx_start
  {
    shift;
    my $storage = shift;
    $storage->sql_do(q/SELECT GET_LOCK("tx", 10)/);
    $storage->std_tx_start(@_);
  }

sub tx_commit
  {
    shift;
    my $storage = shift;
    $storage->std_tx_commit(@_);
    $storage->sql_do(q/SELECT RELEASE_LOCK("tx")/);
  }

sub tx_rollback
  {
    shift;
    my $storage = shift;
    $storage->sql_do(q/SELECT RELEASE_LOCK("tx")/);
    $storage->std_tx_rollback(@_);
  }

Tangram::Dialect::Mysql->register();

1;





