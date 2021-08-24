use Test::Simple tests => 12;

# Test procedure dbms_job.run

# Cleanup garbage from previous regression test runs
`rm -f /tmp/regress_dbms_job.*`;

# Start the scheduler
$ret = `perl bin/pg_dbms_job -c test/regress_dbms_job.conf 2>/dev/null`;
$ret = `ls /tmp/regress_dbms_job.* | wc -l`;
chomp($ret);
ok( $ret eq "2", "pg_dbms_job daemon started");
# Verify that the process is running
$ret = `ps auwx | grep pg_dbms_job: | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "1", "Daemon pg_dbms_job is running");

# Create a job
$ret = `psql -d regress_dbms_job -f test/sql/run.sql > /dev/null 2>&1`;
ok( $? == 0, "Submit job");

# Get the id of the job that have been registered
my $job = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT job FROM dbms_job.all_scheduled_jobs;"`;
chomp($job);
ok( $? == 0 && $job ne "" , "Job $job have been created");

# Mark the job as broken to stop its automatic execution
my $ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user; CALL dbms_job.broken($job, true);"`;
chomp($ret);
ok( $? == 0 && $ret eq "CALL", "Call to broken procedure");
sleep(10);

# Look if the job have been registered in the history table, it should not
my $ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;"`;
chomp($ret);
ok( $? == 0 && $ret eq "0", "Good, no job found in the history");

# Run the job in foreground
my $ret = `psql -d regress_dbms_job -Atc "CALL dbms_job.run($job, false);"`;
chomp($ret);
ok( $? == 0 && $ret eq "CALL", "Call to run procedure for immediate execution of job $job");

# we must have a job registered in the history
my $ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 't1';"`;
chomp($ret);
ok( $? == 0 && $ret eq "1", "Job $job have been executed in foreground");

# Run the job in background
my $ret = `psql -d regress_dbms_job -Atc "CALL dbms_job.run($job, true);"`;
chomp($ret);
ok( $? == 0 && $ret eq "CALL", "Call to run procedure for background execution of job $job");
sleep(10);

# Look if the job have been registered in the history table
my $ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user; SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;"`;
chomp($ret);
ok( $? == 0 && $ret eq "1", "Found $ret async job in the history");

# we must have 2 rows in the t1 table
my $ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user; SELECT count(*) FROM t1;"`;
chomp($ret);
ok( $? == 0 && $ret >= 2, "Job $job have been executed in background, $ret rows");

# Stop the daemon
$ret = `perl bin/pg_dbms_job -c test/regress_dbms_job.conf -k`;
sleep(2);
# Now all process must be terminated
$ret = `ps auwx | grep pg_dbms_job: | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "0", "Daemon pg_dbms_job is stopped");
