use Test::Simple tests => 21;

# Test pg_dbms_job procedures

# Cleanup garbage from previous regression test runs
`rm -f /tmp/regress_dbms_job.*`;

# Start the scheduler
my $ret = `perl bin/pg_dbms_job -c test/regress_dbms_job.conf`;
$ret = `ls /tmp/regress_dbms_job.* | wc -l`;
chomp($ret);
ok( $ret eq "2", "pg_dbms_job daemon started");
# Verify that the process is running
$ret = `ps auwx | grep pg_dbms_job: | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "1", "Daemon pg_dbms_job is running");

# Submit a job that must be executed each days
$ret = `psql -d regress_dbms_job -f test/sql/submit.sql > /dev/null 2>&1`;
ok( $? == 0, "Submit job");
sleep(1);

# Get the id of the job that have been registered
my $job = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT job FROM dbms_job.all_scheduled_jobs;"`;
chomp($job);
ok( $? == 0 && $job ne "" , "Job $job have been created");

# Remove the job
$ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;CALL dbms_job.remove($job);"`;
ok( $? == 0, "Removing job $job");
$ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduled_jobs;"`;
chomp($ret);
ok( $? == 0 && $ret eq "0", "Job $job have been removed");

# Submit the job again
$ret = `psql -d regress_dbms_job -f test/sql/submit.sql > /dev/null 2>&1`;
ok( $? == 0, "Submit job");
sleep(1);

# Get the id of the new job that have been registered
$job = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT job FROM dbms_job.all_scheduled_jobs;"`;
chomp($job);
ok( $? == 0 && $job ne "", "New job $job have been created");

# Change the next execution date to NULL
$ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;CALL dbms_job.next_date($job, NULL);" > /dev/null 2>&1`;
ok( $? != 0, "Can not set next_date to NULL for job $job");

# Change the next execution date to today + 1 year
$ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;CALL dbms_job.next_date($job, date_trunc('day', current_timestamp) + '1 year'::interval);"`;
ok( $? == 0, "Change next_date for job $job");

# Verify that the new next_date that have been registered
$ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT job FROM dbms_job.all_scheduled_jobs WHERE next_date = date_trunc('day', current_timestamp) + '1 year'::interval;"`;
chomp($ret);
ok( $? == 0 && $ret eq $job, "New next_date for job $job have been modified");

# Change the interval to once a month
$ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;CALL dbms_job.interval($job, 'date_trunc(''day'', current_timestamp) + ''1 month''::interval');"`;
ok( $? == 0, "Change interval for job $job");

# Verify that the new interval that have been registered
$ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT job FROM dbms_job.all_scheduled_jobs WHERE md5(interval) = 'fb9412d079a32a090003d1c080619d72';"`;
chomp($ret);
ok( $? == 0 && $ret eq $job, "New interval for job $ret have been modified");

# Change the action to NULL, should be an error
$ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;CALL dbms_job.what($job, NULL);" > /dev/null 2>&1`;
ok( $? != 0, "Change what to NULL for job $job");

# Change the action
$ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;CALL dbms_job.what($job, 'BEGIN PERFORM version(); END;');"`;
ok( $? == 0, "Change what for job $job");

# Verify that the new what value that have been registered
$ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT job FROM dbms_job.all_scheduled_jobs WHERE what = 'BEGIN PERFORM version(); END;';"`;
chomp($ret);
ok( $? == 0 && $ret eq $job, "New what for job $job have been modified");

# Change the modifiable columns to NULL, nothing must be changed
$ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;CALL dbms_job.change($job, NULL, NULL, NULL);"`;
ok( $? == 0, "Change all for job $job to NULL");

# Verify that nothing have changed
$ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduled_jobs WHERE md5(what) = '9e5f15c5784fb71b23e3d6419475d6de' AND md5(interval) = 'fb9412d079a32a090003d1c080619d72' AND next_date = date_trunc('day', current_timestamp) + '1 year'::interval;"`;
chomp($ret);
ok( $? == 0 && $ret eq "1", "Job $job is the same, nothing changed");

# Change the modifiable columns
$ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;CALL dbms_job.change($job, 'VACUUM ANALYZE;', date_trunc('day', current_timestamp) + '1 day'::interval, 'current_timestamp + ''1 day''::interval');"`;
ok( $? == 0, "Change all for job $job");

# Verify that all values have changed
$ret = `psql -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduled_jobs WHERE md5(what) = '4819535deca0cb7a637474c659d1b4e5' AND md5(interval) = 'b508a5fc92a976eac08a9c017b049f92' AND next_date = date_trunc('day', current_timestamp) + '1 day'::interval;"`;
chomp($ret);
ok( $? == 0 && $ret eq "1", "Job $job have been executed and removed");

# Stop the daemon
$ret = `perl bin/pg_dbms_job -c test/regress_dbms_job.conf -k`;
sleep(1);
# We should have the daemon and the child still running,
# the current running jobs must not be stopped
$ret = `ps auwx | grep pg_dbms_job: | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "0", "Daemon pg_dbms_job was stopped");
