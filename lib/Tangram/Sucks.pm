
=head1 NAME

Tangram::Sucks - what there is to be improved in Tangram

=head1 DESCRIPTION

Tangram has taken a concept very familiar to programmers in Java land
to its logical conclusion.

This document is an attempt by the coders of Tangram to summarise the
major problems that are inherant in the design, describe cases for
which the Tangram metaphor does not work well, and list long standing
TO-DO items.

=head2 DESIGN CAVEATS

=over

=item B<query language does not cover all SQL expressions>

=item B<some loss of encapsulation with queries>

It could be said this is not a problem.  After all, adding properties
to a schema of an object is akin to declaring them as "public".

=back

=head2 HARD PROBLEMS

=over

=item B<partial column select>

=item B<no support for SQL UPDATE>

=item B<no explicit support for re-orgs>

=item B<replace SQL expression core>

The whole SQL expression core needs to be replaced with a SQL
abstraction module that is a little better planned.  For instance,
there should be placeholders used in a lot more places.

=item B<support for `large' collections>

Where it is impractical or undesirable to load all of a collection
into memory just to

=back

=head2 MISSING FEATURES

=over

=item B<concise query expressions>

For simple selects, this is too long:

  ...

=item B<non-ID joins>

=item B<tables with no primary key>

=item B<tables with multi-column primary keys>

=item B<tables with auto_increment keys>

=item B<tables without a `type' column>

=item B<tables with custom `type' columns>

=item B<tables with implicit (presence) `type' columns>

=item B<fully symmetric relationships>

back-refs are read-only.

=item B<bulk inserts>

Inserting lots of similar objects should be more efficient.

=item B<`empty subclass' schema support>

You should not need to explicitly add new classes to a schema if a
superclass of them is already in the schema.

=back


=cut

