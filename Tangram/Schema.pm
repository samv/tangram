use strict;

package Tangram::ClassHash;

use Carp;

sub class
{
   my ($self, $class) = @_;
   $self->{$class} or croak "unknown class '$class'";
}

package Tangram::Class;

sub members
{
   my ($self, $type) = @_;
   return @{$self->{$type}};
}

package Tangram::Schema;

use Carp;

use vars qw( %TYPES );

%TYPES = 
(
   %TYPES,
   ref      => new Tangram::Ref,
);

sub new
{
    my $pkg = shift;

	my $self = ref $_[0] ? shift() : { @_ };
    bless $self, $pkg;

    $self->{make_object} ||= sub { shift()->new() };
    $self->{class_table} ||= 'OpalClass';

	$self->{sql}{default_null} ||= 'NULL';
	$self->{sql}{id} ||= 'NUMERIC(15,0)';
	$self->{sql}{cid} ||= 'NUMERIC(5,0)';
	$self->{sql}{oid} ||= 'NUMERIC(10,0)';
	$self->{sql}{cid_size} ||= 4;

    my $types = $self->{types} ||= {};

    %$types = ( %TYPES, %$types );

    my $classes = $self->{'classes'};
    bless $classes, 'Tangram::ClassHash';

    while (my ($class, $def) = each %$classes)
    {
		my $classdef = $classes->{$class};

		bless $classdef, 'Tangram::Class';

		$classdef->{table} ||= $class;

		$classdef->{fields} ||= $classdef->{members};
		$classdef->{members} = $classdef->{fields};

		my $cols = 0;

		foreach my $typetag (keys %{$classdef->{members}})
		{
			my $memdefs = $classdef->{members}{$typetag};
	    
			$memdefs = $classdef->{members}{$typetag} =
			{ map { $_, $_ } @$memdefs } if (ref $memdefs eq 'ARRAY');

			my $type = $self->{types}{$typetag};

			my @members = $types->{$typetag}->reschema($memdefs, $class, $self)
				if $memdefs;

			@{$classdef->{member_type}}{@members} = ($type) x @members;
	    
			@{$classdef->{MEMDEFS}}{keys %$memdefs} = values %$memdefs;
	    
			local $^W = undef;
			$cols += scalar($type->cols($memdefs));
		}

		$classdef->{stateless} = !$cols
			&& (!exists $classdef->{stateless} || $classdef->{stateless});

		foreach my $base (@{$classdef->{bases}})
		{
			push @{$classes->{$base}{specs}}, $class;
		}
    }

    while (my ($class, $classdef) = each %$classes)
    {
		my $root = $class;
	
		while (@{$classes->{$root}{bases}})
		{
			$root = @{$classes->{$root}{bases}}[0];
		}

		$classdef->{root} = $classes->{$root};
		delete $classdef->{stateless} if $root eq $class;
    }

    return $self;
}

sub check_class
{
   my ($self, $class) = @_;
   confess "unknown class '$class'" unless exists $self->{classes}{$class};
}

sub classdef
{
   my ($self, $class) = @_;
   return $self->{classes}{$class} or confess "unknown class '$class'";
}

sub classes
{
   my ($self) = @_;
   return keys %{$self->{'classes'}};
}

sub direct_members
{
   my ($self, $class) = @_;
   return $self->{'classes'}{$class}{member_type};
}

sub all_members
{
   my ($self, $class) = @_;
   my $classes = $self->{'classes'};
	my $members = {};
   
	$self->visit_up($class, sub
	{
		my $direct_members = $classes->{shift()}{member_type};
		@$members{keys %$direct_members} = values %$direct_members;
	} );

	$members;
}

sub all_bases
{
   my ($self, $class) = @_;
   my $classes = $self->{'classes'};
	$self->visit_down($class, sub { @{ $classes->{shift()}{bases} } } );
}

sub find_member
{
   my ($self, $class, $member) = @_;
   my $classes = $self->{'classes'};
   my $result;
   local $@;

   eval
   {
      $self->visit_down($class, sub {
         die if $result = $classes->{shift()}{member_type}{$member}
         })
   };

   $result;
}

sub visit_up
{
   my ($self, $class, $fun) = @_;
   _visit_up($self, $class, $fun, { });
}

sub _visit_up
{
   my ($self, $class, $fun, $done) = @_;
   
   return if $done->{$class};

   my @results = ();

   foreach my $base (@{$self->{'classes'}{$class}{bases}})
   {
      push @results, _visit_up($self, $base, $fun, $done);
   }

   $done->{$class} = 1;

   return @results, &$fun($class);
}

sub visit_down
{
   my ($self, $class, $fun) = @_;
   _visit_down($self, $class, $fun, { });
}

sub _visit_down
{
   my ($self, $class, $fun, $done) = @_;
   
   return if $done->{$class};

   my @results = &$fun($class);

   foreach my $base (@{$self->{'classes'}{$class}{bases}})
   {
      push @results, _visit_down($self, $base, $fun, $done);
   }

   $done->{$class} = 1;

   @results
}

sub for_each_spec
{
   my ($self, $class, $fun) = @_;
   my $done = {};

   foreach my $spec (@{$self->{'classes'}{$class}{specs}})
   {
      _for_each_spec($self, $spec, $fun, $done);
   }
}

sub _for_each_spec
{
   my ($self, $class, $fun, $done) = @_;
   
   return if $done->{$class};

   &$fun($class);
   $done->{$class} = 1;

   foreach my $spec (@{$self->{'classes'}{$class}{specs}})
   {
      _for_each_spec($self, $spec, $fun, $done);
   }

}

sub declare_classes
{
   my ($self, $root) = @_;
   
   foreach my $class ($self->classes)
   {
		my $decl = "package $class;";

      my $bases = @{$self->{classes}{$class}{bases}}
         ? (join ' ', @{$self->{'classes'}{$class}{bases}})
         : $root;

		$decl .= "\@$class\:\:ISA = qw( $bases );" if $bases;

      eval $decl;
   }
}

sub is_persistent
{
   my ($self, $x) = @_;
   my $class = ref($x) || $x;
   return $self->{classes}{$class} && $self->{classes}{$class};
}

1;
