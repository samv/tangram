
package MusicStore;

use CD;
use Tangram::Schema;
use Tangram::IntrArray;
use Tangram::TimePiece;
use Tangram::IntrSet;
use Tangram::Set;

our $schema =
   ({
    classes => [
       CD => {
         fields => {
            string => [ qw(title) ],
            timepiece => [ qw(publishdate) ],
            iarray  => { songs => { class => 'CD::Song',
                                    aggreg => 1,
                                    back => 'cd',
                                  },
                       },
         }
       },
       CD::Song => {
         fields => {
            string => [ qw(name) ],
         }
       },
       CD::Artist => {
         abstract => 1,
         fields => {
            string => [ qw(name popularity) ],
            iset => { cds => { class => 'CD',
                               aggreg => 1,
                               back => 'artist' },
                             },
		   },
       },
       CD::Person => {
         bases  => [ "CD::Artist" ],
         fields => {
            string => [ qw(gender haircolor) ],
            timepiece => [ qw(birthdate) ],
         }
       },
       CD::Band => {
         bases  => [ "CD::Artist" ],
         fields => {
            timepiece => [ qw(creationdate enddate) ],
            set => { members => { class => 'CD::Person',
				  table => "artistgroup",
				},
                   },
	    },
       },
    ],
});

our $pixie_like_schema =
    ({
      classes =>
      [
       # with Heritable::Types, we could use HASH here.
       # in fact, later we will.
       Class::Accessor::Assert =>
       {
	table => "objects",
	sql => { sequence => "oid_sequence" },
	fields => { idbif => undef },
       },
      ],
     });

use Storable qw(dclone);
our $next_gen_schema = dclone $schema;

sub AUTOLOAD {
    my ($func) = ($AUTOLOAD =~ m/.*::(.*)$/);
    return Tangram::Schema->new(${$func})
}

1;
