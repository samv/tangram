# -*- perl -*-

# in this script, we re-org the database by loading in objects from
# one storage and saving them in another.

use lib "t/musicstore";
use Prerequisites;

use Test::More tests => 15;

# to make things interesting, we put the data into a single table,
# which turns our nice relational database into an old school
# heirarchical database.  After all, unless you have a radical schema
# change, this whole operation of a re-org is pretty pointless!

my $storage = DBConfig->vendor->connect
    (MusicStore->schema, DBConfig->cparm);

# some DBI drivers (eg, Informix) don't like two connections from the
# same process
my $storage2 = DBConfig->vendor->connect
    (MusicStore->pixie_like_schema, DBConfig->cparm);

my @classes = qw(CD CD::Artist CD::Song);

# the simplest way would be to use something akin to this:
#
#   $storage2->insert(map { $storage->select($_) } @classes);
#
# however, this exposes one of the caveats with such an "open slander
# insertion" policy.

# If you let any node in an object structure be inserted as an object,
# automatically storing all its sub-trees, there is no easy way to see
# if a given node that is being inserted isn't already a sub-part of
# another stored node.

# Fortunately Tangram::Storage->insert() takes care of this for you.

my @objects = map { $storage->select($_) } @classes;

$storage2->insert(grep { $_->isa("CD") } @objects);

