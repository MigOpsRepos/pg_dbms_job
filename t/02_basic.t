use Test::Simple tests => 15;

# Test that the ademon can be started and stopped
# as well as default privileges on objects

# Cleanup garbage from previous regression test runs
`rm -f /tmp/regress_dbms_job.*`;

# First drop the test database and users
`psql -c "DROP DATABASE regress_dbms_job" 2>/dev/null`;
`psql -c "DROP ROLE regress_dbms_job_user" 2>/dev/null`;
`psql -c "DROP ROLE regress_dbms_job_dba" 2>/dev/null`;

# Create the test scheduler dameon connection user, need to be superuser
my $ret = `psql -c "CREATE ROLE regress_dbms_job_dba LOGIN SUPERUSER PASSWORD 'regress_dbms_job_dba'"`;
ok( $? == 0, "Create regression test supuser: regress_dbms_job_dba");

# Create the test user
$ret = `psql -c "CREATE ROLE regress_dbms_job_user LOGIN PASSWORD 'regress_dbms_job_user'"`;
ok( $? == 0, "Create regression test user: regress_dbms_job_user");


# Create the test database
$ret = `psql -c "CREATE DATABASE regress_dbms_job OWNER regress_dbms_job_dba"`;
ok( $? == 0, "Create test regression database: regress_dbms_job");

# Start the scheduler when the pg_dbms_job extension doesn't exists, it must stop
$ret = `perl bin/pg_dbms_job -c test/regress_dbms_job.conf -s >/dev/null 2>&1`;
$ret = `ls /tmp/regress_dbms_job.* | wc -l`;
chomp($ret);
ok( $ret eq "1", "Scheduler stopped, no pid file");

# Create the schema and object of the pg_dbms_job extension
my $ver = `grep default_version pg_dbms_job.control | sed -E "s/.*'(.*)'/\\1/"`;
chomp($ver);
$ret = `psql -d regress_dbms_job -c "CREATE SCHEMA dbms_job;" > /dev/null 2>&1`;
ok( $? == 0, "Create dbms_job schema");

$ret = `psql -d regress_dbms_job -f sql/pg_dbms_job--$ver.sql > /dev/null 2>&1`;
ok( $? == 0, "Import manually pg_dbms_job extension file");

# Start the scheduler daemon and verify that the pid and log files are created
`perl bin/pg_dbms_job -c test/regress_dbms_job.conf 2>/dev/null`;
$ret = `ls /tmp/regress_dbms_job.* | wc -l`;
chomp($ret);
ok( $ret eq "2" , "Check for pid and log file creation");

# Verify that the process is running
$ret = `ps auwx | grep pg_dbms_job: | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "1", "Deamon pg_dbms_job is running");

# Set privilege to allow user regress_dbms_job_user to work with the extension
$ret = `psql -d regress_dbms_job -c "GRANT USAGE ON SCHEMA dbms_job TO regress_dbms_job_user"`;
ok( $? == 0, "Add privileges to test user on pg_dbms_job schema");
$ret = `psql -d regress_dbms_job -c "GRANT ALL ON ALL TABLES IN SCHEMA dbms_job TO regress_dbms_job_user"`;
ok( $? == 0, "Add privileges to test user on pg_dbms_job tables");
$ret = `psql -d regress_dbms_job -c "GRANT ALL ON ALL SEQUENCES IN SCHEMA dbms_job TO regress_dbms_job_user"`;
ok( $? == 0, "Add privileges to test user on pg_dbms_job sequences");
$ret = `psql -d regress_dbms_job -c "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_job TO regress_dbms_job_user"`;
ok( $? == 0, "Add privileges to test user on pg_dbms_job functions");
$ret = `psql -d regress_dbms_job -c "GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA dbms_job TO regress_dbms_job_user"`;
ok( $? == 0, "Add privileges to test user on pg_dbms_job procedures");

# Stop the daemon
$ret = `perl bin/pg_dbms_job -c test/regress_dbms_job.conf -k`;
$ret = `ls /tmp/regress_dbms_job.pid 2>/dev/null | wc -l`;
chomp($ret);
ok( $ret eq "0", "Check that pid file has been removed");

# Verify that the process is stopped
$ret = `ps auwx | grep pg_dbms_job: | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "0", "Deamon pg_dbms_job is not running");
