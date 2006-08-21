
=head1 NAME

Tangram::Sucks - what there is to be improved in Tangram

=head1 DESCRIPTION

Tangram has taken a concept very familiar to programmers in Java land
to its logical completion.

This document is an attempt by the coders of Tangram to summarise the
major problems that are inherant in the design, describe cases for
which the Tangram metaphor does not work well, and list long standing
TO-DO items.

=head1 DESIGN CAVEATS

=item B<query language does not cover all SQL expressions>

Whilst there is no underlying fault with the query object metaphor
I<per se>, there are currently lots of queries that cannot be
expressed in current versions of Tangram, and adding new parts to the
language is not easy.

=item B<some loss of encapsulation with queries>

It could be said this is not a problem.  After all, adding properties
to a schema of an object is akin to declaring them as "public".

Some people banter on about I<data access patterns>, which the Tangram
schema represents.  But OO terms like that are usually treated as
buzzwords anyway.

=head2 re-orgs require a lot of core

The situation where you have a large amount of schema reshaping to do,
with a complex enough data structure can turn into a fairly difficult
problem.

It is possible to have two Tangram stores with different schema and
simply load objects from one and put them in the other - however the
on-demand autoloading combined with the automatic insertion of unknown
objects will result in the entire database being loaded into core if
it is sufficiently interlinked.

=head1 HARD PROBLEMS

=head2 Partial Column Select

When you partially select columns, you're breaking up your objects
from nice identifiable blobs into scattered fragments of data.  It is
quite easy to write code that breaks preconditions required for
ACID-safe transactions.

Nevertheless, you can do it;

  $storage->select
     ( undef,
       filter => (...),
       retrieve => [ $r_foo->{bar}, ... ]
     );

In principle you could make columns lazy, but this is only implemented
for references and collections.

=head1 MISSING FEATURES

=head2 no support for SQL UPDATE

It may be possible to write a version of C<$storage-E<gt>select()>
that does this, which would look something like:

  $storage->update
      ( $r_object,
        set => [ $r_object->{bar} == $r_object->{baz} + 2 ],
        filter => ($r_object->{frop} != undef)
      );

=head2 replace SQL expression core

The whole SQL expression core needs to be replaced with a SQL
abstraction module that is a little better planned.  For instance,
there should be placeholders used in a lot more places where the code
just sticks in an integer etc.

=head2 support for `large' collections

Where it is impractical or undesirable to load all of a collection
into memory, when you are adding a member and then updating the
container, it should be possible to lazily load individual members as
required, rather than the whole lot.

This could actually be achieved with a new Tangram::Type.  This
problem is particularly visible in the F<t/eg/RT.pm> example in the
Tangram distribution - Queues, for instance, have a set of Tickets,
but you almost never want to load all the tickets in for a queue.
This means that you are creating a relation for the sake of querying,
that is somewhat "dangerous" to actually load.

=head2 concise query expressions

For simple selects, this is too long:

  my $r_foo = $storage->remote("Foo");
  my @bobs = $storage->select( $r_foo, $r_foo->{name} eq "Bob" );

=head2 non-ID joins

Currently, all the L<Tangram::Type::Abstract::Coll>-based classes only
support ID joins.

=head2 tables with no primary key

You need to make a view for Tangram to access, a function to convert
from the row contents to a (possibly hashed) integer 'primary' key.
You also need to write update rules for the view.  It's a bit of a
mess, but can be done.  DB-side C<md5_hex()> support can be used for
this at a stretch.

=head2 tables with multi-column primary keys

Yeah.  Same thing as above; though normally just convert the
multi-column key into an integer somehow.

=head2 tables with auto_increment keys

This is probably quite trivial to add, but nobody cared about it
enough to add it yet.

=head2 tables without a type column

The 'type' column is de-normalized.  What it represents, in principle,
is which tables that share the unique keyspace of this column have
tuples present.  However, instead of being a bitmap or enum set
column, it is an integer index into the perl-side schema.  This leaves
something to be desired.

=head2 tables with custom `type' columns

Quite often you will come across schema that use enumerated or text
columns to indicate typing information.  This is very difficult to map
to Tangram inheritance, but again can be done with clever use of DB
remapping features.

=head2 tables with implicit (presence) `type' columns

Apparently this is what David Wheeler's L<Object::Relational> does.
Instead of having an explicit type indicator, the presence or absence
of rows in other tables determines the result.  This approach uses a
lot of outer joins, but perhaps that is a reasonable pay-off.

=head2 fully symmetric relationships

back-refs are read-only.  There is a complication here that the
Perl-side objects need to also implement the conjoined nature of
symmetric relationships.

=head2 bulk inserts

Inserting lots of similar objects should be more efficient.  Right now
it generates a new STH for each object.

=head2 `empty subclass' schema support

You should not need to explicitly add new classes to a schema if a
superclass of them is already in the schema.

=head1 UNDOCUMENTED

=head2 sub-selects in query expressions

It is possible to make <tt>Tangram::Expr</tt> objects that contain
sub-selects; however the syntax is not documented.

=cut

