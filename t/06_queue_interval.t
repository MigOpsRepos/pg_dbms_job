use Test::Simple tests => 6;

# Submit an asynchronous job and validate that queue_job_interval is respected

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

# Create an asynchronous job that must be executed later in 3 seconds
$ret = `psql -d regress_dbms_job -f test/sql/async_queue_interval.sql > /dev/null 2>&1`;
ok( $? == 0, "Submit job");
sleep(1);

# Look if the job have been registered in the history table, it should not
my $ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;"`;
chomp($ret);
ok( $? == 0 && $ret eq "0", "No async job found in the history: $ret");

# Wait to reach job_queue_interval
sleep(5);

# Now verify that the job have been run
my $ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;"`;
chomp($ret);
ok( $? == 0 && $ret eq "1", "Found $ret async job in the history");

# Stop the daemon
$ret = `perl bin/pg_dbms_job -c test/regress_dbms_job.conf -k`;
sleep(1);
# We should have the daemon and the child still running,
# the current running jobs must not be stopped
$ret = `ps auwx | grep pg_dbms_job: | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "0", "Daemon pg_dbms_job was stopped");
