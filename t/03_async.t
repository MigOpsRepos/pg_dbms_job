use Test::Simple tests => 7;

# Test asynchronous jobs

# Cleanup garbage from previous regression test runs
`rm -f /tmp/regress_dbms_job.*`;

# Start the scheduler
$ret = `perl bin/pg_dbms_job -c test/regress_dbms_job.conf`;
$ret = `ls /tmp/regress_dbms_job.* | wc -l`;
chomp($ret);
ok( $ret eq "2", "pg_dbms_job daemon started");
# Verify that the process is running
$ret = `ps auwx | grep pg_dbms_job: | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "1", "Daemon pg_dbms_job is running");

# Create an asynchronous job
$ret = `psql -d regress_dbms_job -f test/sql/async.sql > /dev/null 2>&1`;
ok( $? == 0, "Submit job");
sleep(2);

# Stop the daemon
$ret = `perl bin/pg_dbms_job -c test/regress_dbms_job.conf -k`;
sleep(1);
# We should have the daemon and the child still running,
# the current running jobs must not be stopped
$ret = `ps auwx | grep pg_dbms_job: | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "2", "Daemon pg_dbms_job and subprocess are still running: $ret");

# Be sure that the job have been processed
sleep(3);

# Verify that the job have been removed from the queue
my $ret = `psql -d regress_dbms_job -Atc "SELECT count(*) FROM dbms_job.all_async_jobs;" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq "0", "Asynchronous job have been removed");

# Look if the job have been registered in the history table
$ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq "1", "Found $ret async job in the history");

sleep(1);

# Now all process must be terminated
$ret = `ps auwx | grep pg_dbms_job: | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "0", "Daemon pg_dbms_job is stopped");
