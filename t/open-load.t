
BEGIN {
    if ( $ENV{DEVELOPER_TEST_RUN_VALGRIND} ) {
        eval "require Test::Valgrind";
        Test::Valgrind->import();
    }
}

use Test::More tests => 1 + 1 + 133 * 2;
use Test::Exception;
use Test::NoWarnings;

use File::Copy qw( cp );

BEGIN { use_ok('CDB::Tiny') };


for my $method ( qw( open load ) ) {
    diag $method;

    my $dbfile = "t/data.cdb";

    my $cdb;

    lives_ok {
        $cdb = CDB::Tiny->$method( $dbfile );
    } "$method";

    eval {
        $cdb->$method( $dbfile );
    };
    like( $@, qr/is already blessed/,
        "open() cannot be called on object reference"
    );

    eval {
        $cdb->put_add( new_key => "some value" );
    };
    like( $@, qr/Database opened in read only mode/,
        "put_add() forbidden in read only mode"
    );

    eval {
        $cdb->put_replace( new_key => "some value" );
    };
    like( $@, qr/Database opened in read only mode/,
        "put_replace() forbidden in read only mode"
    );

    eval {
        $cdb->put_replace0( new_key => "some value" );
    };
    like( $@, qr/Database opened in read only mode/,
        "put_replace0() forbidden in read only mode"
    );

    eval {
        $cdb->put_insert( new_key => "some value" );
    };
    like( $@, qr/Database opened in read only mode/,
        "put_insert() forbidden in read only mode"
    );

    lives_ok {
        $cdb->exists("k12")
    } "exists() available in read only mode";

    is( $cdb->exists("k12"), 1,
        "exists() returns true for existent records"
    );

    my %cdb = cdb_values();
    my %cdb_dups = map {
        my $n = $_;
        ( "k$n" => "v$n" )
    } ( 40 .. 49, 80 .. 89 );

    is( $cdb->get("k45"), 'v45', 'get() returns correct value');
    is( join('|', $cdb->getall("k45")),
        'v45|v45',
        'getall() returns all values'
    );
    is( $cdb->getlast("k45"), 'v45',
        'getlast() returns last value'
    );

    is_deeply( 
        { map { $_ => 1 } $cdb->keys }, { map { $_ => 1 } keys %cdb },
        "keys() returns old and new records"
    );

    while ( my ($k, $v) = $cdb->each ) {
        is( delete $cdb{$k} || delete $cdb_dups{$k}, $v,
            "each() returns correct value for $k"
        );
    }
    is( keys(%cdb) + keys(%cdb_dups), 0, "each() returns all records");
}


sub cdb_values {
    my %v = map {
        my $n = $_;
        $n = "0$n" if length($n) < 2;
        ( "k$n" => "v$n" )
    } ( 0 .. 99 );

    return %v;
}

