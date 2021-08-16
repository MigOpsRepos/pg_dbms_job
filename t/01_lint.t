use Test::Simple tests => 2;

my $ret = `perl -wc bin/pg_dbms_job 2>&1`;
ok( $? == 0, "PERL syntax check");

my $ret = `perl bin/pg_dbms_job --help 2>&1`;
ok( $? == 0, "Program usage");
