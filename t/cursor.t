use strict;
use t::Springfield;

my @kids = qw( Bart Lisa Maggie );
my @population = sort qw( Homer Marge ), @kids;
my $children = 'a_children';

sub NaturalPerson::children
{
   my ($self) = @_;
   return wantarray ? @{ $self->{$children} }
      : join(' ', map { $_->{firstName} } @{ $self->{$children} } )
}

Springfield::begin_tests(8);

{
   my $storage = Springfield::connect_empty;

	my @children = map { NaturalPerson->new( firstName => $_ ) } @kids;

   $storage->insert(
      NaturalPerson->new( firstName => 'Homer', $children => [ @children ] ),
      NaturalPerson->new( firstName => 'Marge', $children => [ @children ] ) );

   $storage->disconnect;
}

Springfield::leaktest;

{
   my $storage = Springfield::connect;

   my $cursor = $storage->cursor( 'NaturalPerson' );
   my @results;

   while (my $person = $cursor->current())
   {
      push @results, $person->{firstName};
      Springfield::test( $person->children eq "@kids" ) if $person->{firstName} eq 'Homer';
      $cursor->next();
   }

   @results = sort @results;

   Springfield::test( "@results" eq "@population" );

   $storage->disconnect;
}

Springfield::leaktest;

{
   my $storage = Springfield::connect;

   my $cursor1 = $storage->cursor( 'NaturalPerson' );
   my $cursor2 = $storage->cursor( 'NaturalPerson' );
   
   my (@r1, @r2);

   while ($cursor1->current())
   {
      my $p1 = $cursor1->current();
      my $p2 = $cursor2->current();
      
      push @r1, $p1->{firstName};
      push @r2, $p2->{firstName};

      Springfield::test( $p1->children eq "@kids" ) if $p1->{firstName} eq 'Homer';
      Springfield::test( $p2->children eq "@kids" ) if $p2->{firstName} eq 'Marge';

      $cursor1->next();
      $cursor2->next();
   }

   @r1 = sort @r1;
   @r2 = sort @r2;

   Springfield::test( "@r1" eq "@population" && "@r2" eq "@population" );

   $storage->disconnect;
}

Springfield::leaktest;
