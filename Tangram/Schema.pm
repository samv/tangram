# (c) Sound Object Logic 2000-2001

use strict;

package Tangram::ClassHash;

use Carp;

sub class
{
   my ($self, $class) = @_;
   $self->{$class} or croak "unknown class '$class'";
}

package Tangram::Node;

sub get_bases
  {
	@{ shift->{BASES} }
  }

*direct_bases = \&get_bases;

sub get_specs
  {
	@{ shift->{SPECS} }
  }

sub for_conforming
{
   my ($class, $fun, @args) = @_;
   my $done = Set::Object->new;

   my $traverse;

   $traverse = sub {
	 my $class = shift;
	 return if $done->includes($class);
	 $done->insert($class);
	 $fun->($class, @args);

	 foreach my $derived (@{ $class->{SPECS} }) {
	   $traverse->($derived);
	 }
   };

   $traverse->($class);
 }

sub for_composing
{
   my ($class, $fun, @args) = @_;
   my $done = Set::Object->new;

   my $traverse;

   $traverse = sub {
	 my $class = shift;
	 return if $done->includes($class);
	 $done->insert($class);

	 foreach my $base (@{ $class->{BASES} }) {
	   $traverse->($base);
	 }

	 $fun->($class, @args);
   };

   $traverse->($class);
 }

sub get_exporter {
  my ($self, $context) = @_;
  
  return $self->{EXPORTER} ||= do {
	
	my (@export_sources, @export_closures);

	
	$self->for_composing( sub {
							my ($part) = @_;

							$context->{class} = $part;
							
							for my $field ($part->direct_fields()) {
							  if (my $exporter = $field->get_exporter($context)) {
								if (ref $exporter) {
								  push @export_closures, $exporter;
								  push @export_sources, 'shift(@closures)->($obj, $context)';
								} else {
								  push @export_sources, $exporter;
								}
							  }
							}
						  } );
	
	my $export_source = join ",\n", @export_sources;
	my $copy_closures = @export_closures ? ' my @closures = @export_closures;' : '';
	
	# $Tangram::TRACE = \*STDOUT;
	
	$export_source = "sub { my (\$obj, \$context) = \@_;$copy_closures\n$export_source }";
	
	print $Tangram::TRACE "Compiling exporter for $self->{name}...\n$export_source\n"
	  if $Tangram::TRACE;
	
	eval $export_source or die;
	}
  }

sub get_importer {
  my ($self, $context) = @_;
  
  return $self->{IMPORTER} ||= do {
	my (@import_sources, @import_closures);
	
	$self->for_composing( sub {
							my ($part) = @_;
							
							$context->{class} = $part;
							
							for my $field ($part->get_direct_fields()) {
							  
							  my $importer = $field->get_importer($context)
								or next;
							  
							  if (ref $importer) {
								push @import_closures, $importer;
								push @import_sources, 'shift(@closures)->($obj, $row, $context)';
							  } else {
								push @import_sources, $importer;
							  }
							}
						  } );
	
	my $import_source = join ";\n", @import_sources;
	my $copy_closures = @import_closures ? ' my @closures = @import_closures;' : '';
	
	# $Tangram::TRACE = \*STDOUT;
	
	$import_source = "sub { my (\$obj, \$row, \$context) = \@_;$copy_closures\n$import_source }";
	
	print $Tangram::TRACE "Compiling importer for $self->{name}...\n$import_source\n"
	  if $Tangram::TRACE;
	
	# use Data::Dumper; print Dumper \@cols;
	eval $import_source or die;
  };
}

package Tangram::Class;
use vars qw(@ISA);
 @ISA = qw( Tangram::Node );

sub members
{
   my ($self, $type) = @_;
   return @{$self->{$type}};
}

sub is_root
  {
	!@{ shift->{BASES} }
  }

sub get_direct_fields
  {
	map { values %$_ } values %{ shift->{fields} }
  }

sub get_table { shift->{table} }

*direct_fields = \&get_direct_fields;

sub get_import_cols {
  my ($self, $context) = @_;
  my $table = $self->{table};
  map { map { [ $table, $_ ] } $_->get_import_cols($context) } $self->get_direct_fields()
}

sub get_export_cols {
  my ($self, $context) = @_;
  my $table = $self->{table};
  map { map { [ $table, $_ ] } $_->get_export_cols($context) } $self->get_direct_fields()
}

package Tangram::Schema;
#our @ISA = qw( SelfLoader );
use Carp;

use vars qw( %TYPES );

%TYPES = 
(
   %TYPES,
#   ref      => new Tangram::Ref,
);

sub new
{
    my $pkg = shift;

	my $self = ref $_[0] ? shift() : { @_ };
    bless $self, $pkg;

    $self->{make_object} ||= sub { shift()->new() };

    $self->{normalize} ||= sub { shift() };
    $self->{class_table} ||= 'OpalClass';

	$self->{control} ||= 'Tangram';

	$self->{sql}{default_null} = 'NULL' unless exists $self->{sql}{default_null};
	$self->{sql}{id_col} ||= 'id';
	$self->{sql}{id} ||= 'INTEGER';
	# commented out because of layout1 compatibility $self->{sql}{class_col} ||= 'type';
	$self->{sql}{cid} ||= 'INTEGER';
	$self->{sql}{oid} ||= 'INTEGER';
	$self->{sql}{cid_size} ||= 4;

    my $types = $self->{types} ||= {};

    %$types = ( %TYPES, %$types );

	my @class_list = ref($self->{'classes'}) eq 'HASH' ? %{ $self->{'classes'} } : @{ $self->{'classes'} };
    my $class_hash = $self->{'classes'} = {};

    bless $class_hash, 'Tangram::ClassHash';

    my $autoid = 0;

    while (my ($class, $def) = splice @class_list, 0, 2)
    {
		my $classdef = $class_hash->{$class} ||= {};
		%$classdef = (%$def, %$classdef);

		if (exists $classdef->{id}) {
		  $autoid = $classdef->{id};
		} else {
		  $classdef->{id} = ++$autoid;
		}

		bless $classdef, 'Tangram::Class';

		$classdef->{name} = $class;
		$classdef->{table} ||= $self->{normalize}->($class, 'tablename');

		$classdef->{fields} ||= $classdef->{members};

		if ( $classdef->{members} and
		     $classdef->{fields} != $classdef->{members} ) {
		    # some other class' definition put something
		    # in our "members" hash.
		    while (
			   my ($type, $fields)
			   = each %{$classdef->{members}}
			  )
		    {
			# so, we have to merge them.  we could use
			#  %{ $classdef->{fields} } =
			#    (%{$classdef->{fields}},
			#     %{$classdef->{members}});
			# but I'm not 100% sure that we will never
			# have common types in the "fields" and
			# "members" hash.
			($classdef->{fields}{$type}{$_}
			 = delete $fields->{$_})
			    foreach (keys %$fields);
		    }
		}
		$classdef->{members} = $classdef->{fields};

		my $cols = 0;

		foreach my $typetag (keys %{$classdef->{members}})
		{
			my $memdefs = $classdef->{members}{$typetag};
	    
			$memdefs = $classdef->{members}{$typetag} =
			{ map { $_, $_ } @$memdefs } if (ref $memdefs eq 'ARRAY');

			my $type = $self->{types}{$typetag};

			croak("Unknow field type '$typetag', ",
				  "did you forget some 'use Tangram::SomeType' ",
				  "in your program?\n")
				unless defined $types->{$typetag};

			my @members = $types->{$typetag}->reschema($memdefs, $class, $self)
				if $memdefs;

			for my $field (keys %$memdefs) {
			  $memdefs->{$field}{name} = $field;
			  my $fielddef = bless $memdefs->{$field}, ref $type;
			  my @cols = $fielddef->get_export_cols( {} );
			  $cols += @cols;
			}

			@{$classdef->{member_type}}{@members} = ($type) x @members;
	    
			@{$classdef->{MEMDEFS}}{keys %$memdefs} = values %$memdefs;
		}

		$classdef->{stateless} = !$cols
			&& (!exists $classdef->{stateless} || $classdef->{stateless});

		foreach my $base (@{$classdef->{bases}})
		{
			push @{$class_hash->{$base}{specs}}, $class;
		}
    }

    while (my ($class, $classdef) = each %$class_hash)
    {
		my $root = $class;
	
		while (@{$class_hash->{$root}{bases}})
		{
			$root = @{$class_hash->{$root}{bases}}[0];
		}

		$classdef->{root} = $class_hash->{$root};
		delete $classdef->{stateless} if $root eq $class;

		$classdef->{BASES} = [ map { $class_hash->{$_} } @{ $classdef->{bases} } ];
		$classdef->{SPECS} = [ map { $class_hash->{$_} } @{ $classdef->{specs} } ];
		
		if (0) { # currently causes 'panic: magic_killbackrefs, <CONFIG> line 1 during global destruction.'
		  for my $ref (@{ $classdef->{SPECS} }) {
			Tangram::weaken $ref;
		  }
		}
    }

    return $self;
}

sub all_classes
  {
	return values %{ shift->{classes} };
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

*get_class_by_name = \&classdef;

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

sub find_member_class
{
   my ($self, $class, $member) = @_;
   my $classes = $self->{'classes'};
   my $result;
   local $@;

   eval
   {
      $self->visit_down($class,
         sub
         {
            my $class = shift;

            if (exists $classes->{$class}{member_type}{$member})
            {
               $result = $class;
               die;
            }
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

sub for_bases
{
   my ($self, $class, $fun) = @_;
   my %done;
   my $classes = $self->{classes};

   my $traverse;

   $traverse = sub {
	 my $class = shift;
	 return if $done{$class}++;
	 my $def = $classes->{$class};

	 foreach my $base (@{ $def->{bases} }) {
	   $traverse->($base);
	 }

	 $fun->($def);
   };

   foreach my $base (@{ $classes->{$class}{bases} }) {
	 $traverse->($base);
   }
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

#use SelfLoader;
#sub DESTROY { }

1;

