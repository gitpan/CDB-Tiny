
BEGIN {
    if ( $ENV{DEVELOPER_TEST_RUN_VALGRIND} ) {
        eval "require Test::Valgrind";
        Test::Valgrind->import();
    }
}

use Test::More tests => 1 + 1 + 61 * 2;
use Test::Exception;
use Test::NoWarnings;

use File::Copy qw( cp );

BEGIN { use_ok('CDB::Tiny') };

my $dbfileorig = "t/data.cdb";
my $dbfile = "t/data4cr.cdb";

# shared refs
{
    package CDB::Tiny::Test::Package1;
    sub new { bless {name=>__PACKAGE__}, shift };
}
{
    package CDB::Tiny::Test::Package2;
    use overload '""' => sub { shift->{name} };
    sub new { bless {name=>__PACKAGE__}, shift };
}

open(BIN_FILE, $dbfileorig) or die "Cannot open binary file $dbfileorig: $!";
binmode(BIN_FILE);
my $binary_data = '';
{
    local $/;
    my $buf;
    while ( (my $n = read BIN_FILE, $buf, 20) != 0) {
        $binary_data .= $buf;
    }
}

my %refs = (
    scalar_ref => \'new_value1',
    array_ref => [qw( foo bar )],
    hash_ref => { foo => 'bar' },
    code_ref => sub { print "hello" },
    glob_ref => \*BIN_FILE,
    obj_ref1 => CDB::Tiny::Test::Package1->new,
    obj_ref2 => CDB::Tiny::Test::Package2->new,
);


for my $method ( qw( open load ) ) {
    diag $method;

    cp( $dbfileorig, $dbfile )
        or die "Cannot create a copy of t/data.cdb: $!\n";

    my $cdb;

    lives_ok {
        $cdb = CDB::Tiny->$method( $dbfile, for_create => "$dbfile.$$" );
    } "$method(for_create)";

    eval {
        $cdb->get("k12");
    };
    like( $@, qr/Database changes not written yet/,
        "get() unavailable in create mode"
    );

    eval {
        $cdb->getall("k12");
    };
    like( $@, qr/Database changes not written yet/,
        "getall() unavailable in create mode"
    );

    eval {
        $cdb->getlast("k12");
    };
    like( $@, qr/Database changes not written yet/,
        "getlast() unavailable in create mode"
    );

    eval {
        $cdb->keys();
    };
    like( $@, qr/Database changes not written yet/,
        "keys() unavailable in create mode"
    );

    eval {
        $cdb->each();
    };
    like( $@, qr/Database changes not written yet/,
        "each() unavailable in create mode"
    );

    lives_ok {
        $cdb->exists("k12")
    } "exists() available before any changes made";

    is( $cdb->exists("k12"), 0,
        "exists() returns false for non-existent records"
    );

    # put_*


    lives_ok {
        $cdb->put_add( new => 'value' );
    } "put_add() is the only method allowed";

    is(
        $cdb->put_add(
            new_key1 => 'new_value1',
            new_key2 => 'new_value2',
            new_key3 => 'new_value3',
        ), 3,
        "put_add() returns correct number of new records added"
    );

    is(
        $cdb->put_add(
            new_key2 => 'new_value2 - added again',
            new_key3 => 'new_value3 - added again',
        ), 2,
        "put_add() works for just added keys "
    );

    is( $cdb->put_add( binary_data => $binary_data ), 1,
        "put_add() works fine with binary data"
    );

    is(
        $cdb->put_add( %refs ), 7,
        "put_add() works for references - stringified values are stored"
    );

    is( $cdb->put_add( binary_data => $binary_data ), 1,
        "put_add() works fine with binary data (added again)"
    );

    is(
        $cdb->put_replace( non_existent_key => 'brand_new_value' ), 0,
        "put_replace() adds non existent keys and returns 0 (records replaced)"
    );

    is(
        $cdb->put_replace0( put_replace0 => 'that will be replaced later' ), 0,
        "put_replace0() adds non existent keys and returns 0 (records replaced)"
    );


    is(
        $cdb->put_replace( non_existent_key => 'brand_new_value2' ), 1,
        "put_replace() tells how many records have been replaced"
    );

    is(
        $cdb->put_replace0( put_replace0 => 'previous entry filled with zeros' ), 1,
        "put_replace0() tells how many records have been replaced"
    );

    is(
        $cdb->put_replace0( replace0_2 => 'this will be deleted later' ), 0,
        "put_replace0() adds non existent keys and returns 0 (records replaced)"
    );

    is(
        $cdb->put_replace0( replace0_2 => 'previous entry deleted' ), 1,
        "put_replace0() of last record works same way as put_replace()"
    );

    is(
        $cdb->put_insert(
            that_cant_exist_before => 'inserted record',
        ), 1,
        "put_insert() returns number of rows added"
    );

    is( $cdb->exists("that_cant_exist_before"), 1,
        "exists() returns true for just inserted records"
    );

    eval {
        $cdb->put_insert(
            new_key1 => 'that record already exists',
        );
    };
    like ( $@ , qr/Unable to insert new record - key exists/,
        "put_insert() cannot add duplicated records"
    );

    {
        my $warning;
        local $SIG{__WARN__} = sub {
            $warning = shift;
        };

        my $res = $cdb->put_warn(
            new_key1 => 'that record already exists',
            k1234567 => 'that record is new',
        );
        like( $warning, qr/Key new_key1 already exists - added anyway/,
            "put_warn() warns about duplicated entries, but adds them anyway" 
        );

        is( $res, 2, "put_warn() returns number of rows added");
    };

    eval {
        $cdb->finish(invalid_option => 1);
    };
    like( $@, qr/Invalid option/, "finish() won't accept invalid options");

    lives_ok {
        $cdb->finish( reopen => 1, save_changes => 1 ),
    } "finish() saves changes and reopens db";

    eval {
        $cdb->put_add( new_key => "some value" );
    };
    like( $@, qr/Database changes already committed/,
        "put_add() forbidden after finish() was called"
    );

    eval {
        $cdb->put_replace( new_key => "some value" );
    };
    like( $@, qr/Database changes already committed/,
        "put_replace() forbidden after finish() was called"
    );

    eval {
        $cdb->put_replace0( new_key => "some value" );
    };
    like( $@, qr/Database changes already committed/,
        "put_replace0() forbidden after finish() was called"
    );

    eval {
        $cdb->put_insert( new_key => "some value" );
    };
    like( $@, qr/Database changes already committed/,
        "put_insert() forbidden after finish() was called"
    );

    is( $cdb->exists("put_replace0"), 1,
        "exists() allowed after changes committed"
    );

    my %cdb = cdb_values();
    my %cdb_dups = (
        new_key1 => 'that record already exists',
        new_key2 => 'new_value2 - added again',
        new_key3 => 'new_value3 - added again',
        binary_data => $binary_data,
    );

    is( $cdb->get("new_key1"), 'new_value1', 'get() returns correct value');
    is( join('|', $cdb->getall("new_key1")),
        'new_value1|that record already exists',
        'getall() returns all values'
    );
    is( $cdb->getlast("new_key1"), 'that record already exists',
        'getlast() returns last value'
    );

    is( $cdb->get("binary_data"), $binary_data,
        "get() returns correct binary data"
    );

    is( join('|',$cdb->getall("binary_data")),
         join('|', $binary_data, $binary_data),
        "getall() returns correct binary data"
    );

    is( $cdb->getlast("binary_data"), $binary_data,
        "getlast() returns correct binary data"
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
    my %v = (
        (
            map {
                $_ => "$refs{$_}" # stringify
            } keys %refs,
        ),
        binary_data => $binary_data,
        new => 'value',
        new_key1 => 'new_value1',
        new_key2 => 'new_value2',
        new_key3 => 'new_value3',
        non_existent_key => 'brand_new_value',
        put_replace0 => 'that will be replaced later',
        non_existent_key => 'brand_new_value2',
        put_replace0 => 'previous entry filled with zeros',
        replace0_2 => 'this will be deleted later',
        replace0_2 => 'previous entry deleted',
        that_cant_exist_before => 'inserted record',
        k1234567 => 'that record is new',
    );

    return %v;
}


END {
    close(BIN_FILE);
    unlink $dbfile;
};

