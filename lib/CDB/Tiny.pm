package CDB::Tiny;

use 5.007003;
use strict;
use warnings;

require Exporter;

our @ISA = qw(Exporter);

our @EXPORT_OK = ();

our @EXPORT = qw();

our $VERSION = '0.01';

require XSLoader;
XSLoader::load('CDB::Tiny', $VERSION);

1;
__END__

=encoding utf8

=head1 NAME

CDB::Tiny - Perl extension for TinyCDB library to cdb databases

=head1 SYNOPSIS

  use CDB::Tiny;

  # open ( direct file access )
  my $cdb = CDB::Tiny->open( 'my.cdb' );

  # load ( loads file into memory )
  my $cdb = CDB::Tiny->load( 'my.cdb' );

  # returns first occurence of key in file
  print $cdb->get("key");

  # returns all records for key
  print "$_\n" for $cdb->getall("key");

  # returns last record for key
  print $cdb->getlast("key");

  # checks if key exists
  print $cdb->exists("key");

  # iterates over all entries
  while ( my ($key, $value) = $cdb->each ) {
    # same as cdb -d my.cdb, but skips null keys
    printf("+%d,%d:%s->%s\n", length($key), length($value), $key, $value);
  }

  # returns all keys (skips null keys)
  print "$_\n" for $cdb->keys;

or

  # open/load for updating - loads all existing records into temp file
  my $cdb = CDB::Tiny->open( 'my.cdb', for_update => "my.cdb.$$" );
  my $cdb = CDB::Tiny->load( 'my.cdb', for_update => "my.cdb.$$" );

  # add new records (allows duplicates)
  print "records added: ", $cdb->put_add( k1, 'value1'); # 1
  print "records added: ", $cdb->put_add( k1 => 'value1', k2 => 'value2' ); # 2

  # replace and remove old records
  print "records replaced: ", $cdb->put_replace( k3, 'value3'); # 0
  print "records replaced: ", $cdb->put_replace( key, 'value'); # 1

  # replace and fill with null old records
  print "records replaced: ", $cdb->put_replace0( k2, 'value3'); # 1

  # add and warn if record previously existed
  print "records added: ", $cdb->put_warn( k1, 'value4'); # 1
  # warns: Key k1 already exists - added anyway

  # checks if key exists
  print $cdb->exists("k1");

  # dies if record already existed
  eval {
      $cdb->put_insert( k1, 'value1');
  };
  if ( $@ ) {
    print "k1 wasn't added: $@\n"; # Unable to insert new record - key exists
  }
  
  # commit changes and reopen/reload db - temp file replaces my.cdb
  $cdb->finish( save_changes => 1, reopen => 1);
  # same as
  $cdb->finish();

  print $cdb->getlast("k1"); # value4 

  # finish without saving changes
  $cdb->finish( save_changes => 0 );

  # finish without reopening file
  $cdb->finish( reopen => 0 );

or

  # open/load for create - loads all existing records into temp file
  my $cdb = CDB::Tiny->open( 'my.cdb', for_create => "my.cdb.$$" );
  my $cdb = CDB::Tiny->load( 'my.cdb', for_create => "my.cdb.$$" );

  # add new records (allows duplicates)
  print "records added: ", $cdb->put_add( k1, 'value1'); # 1
  print "records added: ", $cdb->put_add( k1 => 'value1', k2 => 'value2' ); # 2

  # replace and remove old records
  print "records replaced: ", $cdb->put_replace( k3, 'value3'); # 0
  print "records replaced: ", $cdb->put_replace( key, 'value'); # 1

  # replace and fill with null old records
  print "records replaced: ", $cdb->put_replace0( k2, 'value3'); # 1

  # add and warn if record previously existed
  print "records added: ", $cdb->put_warn( k1, 'value4'); # 1
  # warns: Key k1 already exists - added anyway

  # checks if key exists
  print $cdb->exists("k1");

  # dies if record already existed
  eval {
      $cdb->put_insert( k1, 'value1');
  };
  if ( $@ ) {
    print "k1 wasn't added: $@\n"; # Unable to insert new record - key exists
  }
  
  # commit changes and reopen/reload db - temp file replaces my.cdb
  $cdb->finish( save_changes => 1, reopen => 0);
  # same as
  $cdb->finish();

  # finish without saving changes
  $cdb->finish( save_changes => 0 );

  # reading is not allowed after finish()
  print $cdb->getlast("k1"); # dies

or

  # create new cdb file
  my $cdb = CDB::Tiny->create( 'my-new.cdb', "my-new.cdb.$$" );

  # add new records (allows duplicates)
  print "records added: ", $cdb->put_add( k1, 'value1'); # 1
  print "records added: ", $cdb->put_add( k1 => 'value1', k2 => 'value2' ); # 2

  # replace and remove old records
  print "records replaced: ", $cdb->put_replace( k3, 'value3'); # 0
  print "records replaced: ", $cdb->put_replace( key, 'value'); # 1

  # replace and fill with null old records
  print "records replaced: ", $cdb->put_replace0( k2, 'value3'); # 1

  # add and warn if record previously existed
  print "records added: ", $cdb->put_warn( k1, 'value4'); # 1
  # warns: Key k1 already exists - added anyway

  # checks if key exists
  print $cdb->exists("k1");

  # dies if record already existed
  eval {
      $cdb->put_insert( k1, 'value1');
  };
  if ( $@ ) {
    print "k1 wasn't added: $@\n"; # Unable to insert new record - key exists
  }
  
  # commit changes and reopen/reload db - temp file replaces my-new.cdb
  $cdb->finish( save_changes => 1, reopen => 0);
  # same as
  $cdb->finish();

  # reading is not allowed in create mode
  print $cdb->getlast("k1"); # dies


=head1 DESCRIPTION

CDB::Tiny is a perl extension for TinyCDB library to query and create CDB
files;

=head2 EXPORT

None by default.

=head1 SEE ALSO

L<CDB_File>
L<http://www.corpit.ru/mjt/tinycdb.html>

=head1 AUTHOR

Alex J. G. Burzyński, E<lt>ajgb@cpan.org<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2010 by AJGB

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.0 or,
at your option, any later version of Perl 5 you may have available.


=cut
