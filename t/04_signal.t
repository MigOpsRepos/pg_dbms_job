use Test::Simple tests => 8;

# Test reload signal and interrupt, kill have been tested.

# Cleanup garbage from previous regression test runs
`rm -f /tmp/regress_dbms_job.*`;

# Cleanup history table
$ret = `psql -d regress_dbms_job -c "TRUNCATE dbms_job.all_scheduler_job_run_details" > /dev/null 2>&1`;
ok( $? == 0, "truncate table dbms_job.all_scheduler_job_run_details");

# Start the scheduler
$ret = `perl bin/pg_dbms_job -c test/regress_dbms_job.conf`;
$ret = `ps auwx | grep pg_dbms_job | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "1", "Daemon pg_dbms_job is running");

# Create an asynchronous job
$ret = `psql -d regress_dbms_job -f test/sql/async.sql > /dev/null 2>&1`;
ok( $? == 0, "Import pg_dbms_job schema");
sleep(2);

# Reload the daemon
$ret = `perl bin/pg_dbms_job -c test/regress_dbms_job.conf -r`;
sleep(1);
# We should have the daemon and the child still running,
$ret = `ps auwx | grep pg_dbms_job | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "2", "Daemon pg_dbms_job and subprocess are still running: $ret");

# Be sure that the job have been processed
sleep(3);

# Look if the job have been registered in the history table
my $ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;"`;
chomp($ret);
ok( $? == 0 && $ret eq "1", "Found $ret async job in the history");

# Create another asynchronous job
$ret = `psql -d regress_dbms_job -f test/sql/async.sql > /dev/null 2>&1`;
ok( $? == 0, "Import pg_dbms_job schema");
sleep(2);

# Interrupt immediatly the daemon
$ret = `perl bin/pg_dbms_job -c test/regress_dbms_job.conf -m`;
sleep(5);

# Now all process must be terminated
$ret = `ps auwx | grep pg_dbms_job | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "0", "Daemon pg_dbms_job is interrupted: $ret");

# Look if the last job have been registered in the history table, it must not
my $ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;"`;
chomp($ret);
ok( $? == 0 && $ret eq "1", "Found $ret async job in the history");
