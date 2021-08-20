use Test::Simple tests => 11;

# Cleanup garbage from previous regression test runs
`rm -f /tmp/regress_dbms_job.*`;

# Submit an asynchronous job and validate that queue_job_interval is respected

# Start the scheduler
$ret = `perl bin/pg_dbms_job -c test/regress_dbms_job.conf`;
$ret = `ls /tmp/regress_dbms_job.* | wc -l`;
chomp($ret);
ok( $ret eq "2", "pg_dbms_job daemon started");
# Verify that the process is running because of privilege issues
$ret = `ps auwx | grep pg_dbms_job: | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "1", "Daemon pg_dbms_job is not running");

# Create an scheduled job that must be executed later in 3 seconds and each 6 seconds after
$ret = `psql -d regress_dbms_job -f test/sql/scheduled.sql > /dev/null 2>&1`;
ok( $? == 0, "Submit job");
sleep(1);

# Look if the job have been registered in the history table, it should not
my $ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;"`;
chomp($ret);
ok( $? == 0 && $ret eq "0", "No async job found in the history: $ret");

# Wait to reach job_queue_interval
sleep(7);

# Now verify that the job have been run
my $ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;"`;
chomp($ret);
ok( $? == 0 && $ret eq "1", "Found $ret async job in the history");

# Wait that at least 2 more job execution was done (12 seconds)
sleep(15);

# Now verify that we have 2 jobs that have been run
my $ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;"`;
chomp($ret);
ok( $? == 0 && $ret eq "2", "Found $ret async job in the history");

sleep(6);

# Mark the job as broken to stop its execution, we should have a third trace in the history
my $ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user; CALL dbms_job.broken(6, true);"`;
chomp($ret);
ok( $? == 0 && $ret eq "CALL", "Call to broken procedure");

sleep(15);

# Now verify that we still have 3 jobs that have been run
my $ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;"`;
chomp($ret);
ok( $? == 0 && $ret eq "3", "Found $ret async job in the history");

# Mark the job as not broken to restart its execution
my $ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user; CALL dbms_job.broken(6, false);"`;
chomp($ret);
ok( $? == 0 && $ret eq "CALL", "Call to broken procedure");

sleep(15);

# Now verify that we have 5 jobs that have been run
my $ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;"`;
chomp($ret);
ok( $? == 0 && $ret eq "5", "Found $ret async job in the history");

# Stop the daemon
$ret = `perl bin/pg_dbms_job -c test/regress_dbms_job.conf -k`;
sleep(1);
# We should have the daemon and the child still running,
# the current running jobs must not be stopped
$ret = `ps auwx | grep pg_dbms_job: | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "0", "Daemon pg_dbms_job was stopped");


