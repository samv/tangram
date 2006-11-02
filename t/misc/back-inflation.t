#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 10;

BEGIN {
    use_ok('Tangram');
    use_ok('Class::Tangram::Generator');
};

my $schemahash = {
    classes => {
       Hat => {
            fields => {
                string   => [ qw( colour ) ],
            },
       },
       NaturalPerson => {
            fields => {
                string   => [ qw( firstName name ) ],
                int      => [ qw( age ) ],
                ref      => { partner => { null => 1 } },
                iset    => {
                    hats => {
                        class => 'Hat',
                        back => 'owner',
                    },
                },
            },
       },
    }
};

my $schema = Tangram::Schema->new($schemahash);
my $file = "/tmp/tangram$$.db";
my @cp = ("dbi:SQLite:$file");

unlink($file);
Tangram::Relational->deploy($schema, @cp);
my $gen     = Class::Tangram::Generator->new($schema);
my $storage = Tangram::Relational->connect($schema, @cp);

my $hat = $gen->new('Hat', colour => 'blue');
my $person = $gen->new('NaturalPerson', name => 'tangram');
$person->hats->insert($hat);
ok(scalar $person->hats, 'hat given to owner');

$storage->insert($person);

undef $person;
undef $hat;

($person) = $storage->select('NaturalPerson');
ok(ref($person) eq 'NaturalPerson', 'person inserted and retrieved');

($hat) = $person->hats;
ok(ref($hat) eq 'Hat', 'person has a hat');

(my $owner) = $hat->owner;
ok(ref($owner) eq 'NaturalPerson', 'owner of hat is a person');

ok(@{$owner->hats}, 'owner of hat has hats');


my $rem = $storage->remote('Hat');
($hat) = $storage->select($rem, $rem->{owner} eq $person);
ok(ref($hat) eq 'Hat', 'hat inserted and retrieved');

($owner) = $hat->owner;
ok(ref($owner) eq 'NaturalPerson', 'owner of hat is a person');

ok(@{$owner->hats}, 'owner of hat has hats');

