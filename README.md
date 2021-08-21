# pg_dbms_job

## Description

This PostgreSQL extension provided full compatibility with the DBMS_JOB Oracle module.

It allows the creation, scheduling, and managing of jobs. A job runs a SQL command, a stored procedure or any plpgsql code which has been previously stored in the database. The `submit` stored procedure is used to create and store the definition of a job. A job identifier is assigned to a job along with its associated code to execute and the attributes describing when and how often the job is to be run.

If the `submit` stored procedure is called without the `next_date` (when) and `interval` (how often) attributes, the job is executed immediatly in an asynchronous process. If `interval` is NULL and that `next_date` is lower or equal to current timestamp the job is also executed immediatly in an asynchronous process. If the 'when' and 'how often' attributes are set the job will be started when appropriate.

If a scheduled job completes successfully, then its new execution date is placed in `next_date`. The nuew date is calculated by executing the statement `SELECT interval INTO next_date`. The `interval` parameter must evaluate to a time in the future.

This extension consist in a SQL script to create all the objects related to its operation and a daemon that must be run attached to the database where job are defined. The daemon is responsible to execute the queued asynchronous jobs and the scheduled ones. It should be running on the same host or both host should have the same time synchronization source.

The number of job that can be executed at the same time is limited to 1000.

## Installation

There is no special requirement to run this extension but your PostgreSQL version must support extensions (>= 9.1) and Perl must be available as well as the DBI and DBD::Pg Perl modules. If your distribution doesn't include these Perl modules you can always install them using CPAN:

    perl -MCPAN -e 'install DBI'
    perl -MCPAN -e 'install DBD::Pg'

or in Debian like distribution use:

    apt install libdbi-perl libpg-perl

and on RPM based system:

    yum install perl-DBI perl-DBD-Pg

To install the extension execute

    make
    sudo make install

Test of the extension can be done using:

    make installcheck

## Create/upgrade the extension

Each database that needs to use `pg_dbms_job` must creates the extension:

    psql -d mydb -c "CREATE EXTENSION pg_dbms_job"

To upgrade to a new version execute:

    psql -d mydb -c 'ALTER EXTENSION pg_dbms_job UPDATE TO "1.1.0"'

If you doesn't have the privileges to create an extension you can just import the extension file into the database, for example:

    psql -d mydb -f sql/pg_dbms_job--1.0.0.sql

This is especially useful for database in DBaas cloud services. To upgrade just import the extension upgrade files using psql.

A dedicated scheduler per database using the extentionmust be started.

## Running the scheduler

The scheduler is a Perl program that runs in background it can be executed by any system user as follow:

    pg_dbms_job -c /etc/pg_dbms_job/mydb-dbms_job.conf

There must be one scheduler daemon running per database using the extension with a dedicated configuration file.

The configuration file must define the database connection settings where the pg_dbms_job extension is used. This connection must be the extension tables owner or have the superuser privileges to be able to bypass the Row Level Security rules defined on the pg_dbms_job tables.

```
usage: pg_dbms_job [options]

options:

  -c, --config  file  configuration file. Default: /etc/pg_dbms_job/pg_dbms_job.conf
  -d, --debug         run in debug mode.
  -k, --kill          stop current running daemon gracefully waiting
                      for all job completion.
  -m, --immediate     stop running daemon and jobs immediatly.
  -r, --reload        reload configuration file and jobs definition.
  -s, --single        do not detach and run in single loop mode and exit.
```

### Configuration

The format of the configuration file is the same as postgresql.conf.

#### General

- `debug`: debug mode. Default 0, disabled.
- `pidfile`: path to pid file. Default to `/tmp/pg_dbms_job.pid`
- `logfile`: path to log file. Default `/tmp/pg_dbms_job.log`

#### Database

- `host`: ip adresse or hostname where the PostgreSQL cluster is running.
- `port`: port where the PostgreSQL cluster is listening.
- `database`: name of the database where to connect.
- `user`: username used to connec to the database, it must be a superuser role.
- `passwd`: password for this role.

#### Example
```
#-------------
#  General
#-------------
# Toogle debug mode
debug=0
# Path to the pid file
pidfile=/tmp/pg_dbms_job.pid
# log file
logfile=/tmp/pg_dbms_job.log

#-------------
#  Database
#-------------
# Information of the database to poll
host=localhost
port=5432
database=dbms_job
user=gilles
passwd=gilles
```

## Jobs definition

### Scheduled jobs

Jobs to run are stored in table `dbms_job.all_scheduled_jobs` which is the same structure as the one in Oracle. Some columns are just here for compatibility but are not used. They are executed when current timestamp of the scheduler daemon is upper or equal to the date defined in the `next_date` attribute.

```
CREATE TABLE dbms_job.all_scheduled_jobs
(
	job bigint DEFAULT nextval('dbms_job.jobseq') PRIMARY KEY, -- identifier of job
	log_user name DEFAULT current_user, -- user that submit the job
	priv_user name DEFAULT current_user, -- user whose default privileges apply to this job (not used)
	schema_user text DEFAULT current_setting('search_path'), -- default schema used to parse the job
	last_date timestamp with time zone, -- date on which this job last successfully executed
	last_sec text, -- same as last_date (not used)
	this_date timestamp with time zone, -- date that this job started executing
	this_sec text, -- same as this_date (not used)
	next_date timestamp(0) with time zone NOT NULL, -- date that this job will next be executed
	next_sec timestamp with time zone, -- same as next_date (not used)
	total_time interval, -- total wall clock time spent by the system on this job, in seconds
	broken boolean DEFAULT false, -- true: no attempt is made to run this job, false: an attempt is made to run this job
	interval text, -- a date function, evaluated at the start of execution, becomes next next_date
	failures bigint, -- number of times the job has started and failed since its last success
	what text  NOT NULL, -- body of the anonymous pl/sql block that the job executes
	nls_env text, -- session parameters describing the nls environment of the job (not used)
	misc_env bytea, -- Other session parameters that apply to this job (not used)
	instance integer DEFAULT 0 -- ID of the instance that can execute or is executing the job (not used)
);
```

### Asynchronous job

Job submitted without execution date are jobs that need to be executed asynchronously as soon as possible after being created. They are stored in the queue (FIFO) table `dbms_job.all_async_jobs`.
```
CREATE TABLE dbms_job.all_async_jobs
(
        job bigint DEFAULT nextval('dbms_job.jobseq') PRIMARY KEY, -- identifier of job
        log_user name DEFAULT current_user, -- user that submit the job
        schema_user text DEFAULT current_setting('search_path'), -- default search_path used to execute the job
        create_date timestamp with time zone DEFAULT current_timestamp, -- date on which this job has been created.
        what text NOT NULL -- body of the anonymous pl/sql block that the job executes
);
```
## View DMBS_JOB.ALL_JOBS

All jobs that have to be executed can be listed from the view `dbms_job.all_jobs`, this is the equivalent of the Oracle table DBMS_JOB.ALL_JOBS. THis view reports all jobs to be run by execution a union between the two tables described in previous chapters.

## Security

Jobs are only visible by their own creator. A user can not access to a job defined by an other user unless it has the superuser privileges or it is the owner of the pg_dbms_job tables.

By default a user can not use pg_dbms_job, he must be granted privileges to the pg_dbms_job objects as follow.

```
GRANT USAGE ON SCHEMA dbms_job TO <role>;
GRANT ALL ON ALL TABLES IN SCHEMA dbms_job TO <role>;
GRANT ALL ON ALL SEQUENCES IN SCHEMA dbms_job TO <role>;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_job TO <role>;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA dbms_job TO <role>;
```

A job will be taken in account by the scheduler only when the transaction where it has been created is committed. It is transactionnal so no risk that it will be executed if the transaction is aborted.

When starting or when it is reloaded the pg_dbms_job daemon first checks that another daemon is not already attached to the same database. If this is the case it will refuse to continue. This is a double verification, the first one is on an existing pid file and the second is done by looking at pg_stat_activity to see if a `pg_dbms_job:main` process already exists.

## Job history

Oracle DBMS_JOB doesn't provide a log history. This feature is available in DBMS_SCHEDULER and the past activity of nthe scheduler can be seen in table ALL_SCHEDULER_JOB_RUN_DETAILS. This extension stores all PG_DBMS_JOB activity in a similar table named `dbms_job.all_scheduler_job_run_details`.

```
CREATE TABLE dbms_job.all_scheduler_job_run_details
(
        log_id bigserial PRIMARY KEY, -- unique identifier of the log entry
        log_date timestamp with time zone DEFAULT current_timestamp, -- date of the log entry
        owner name, -- owner of the scheduler job
        job_name varchar(261), -- name of the scheduler job
        job_subname varchar(261), -- Subname of the Scheduler job (for a chain step job)
        status varchar(128), -- status of the job run
        error char(5), -- error code in the case of an error
        req_start_date timestamp with time zone, -- requested start date of the job run
        actual_start_date timestamp with time zone, -- actual date on which the job was run
        run_duration bigint, -- duration of the job run
        instance_id integer, -- identifier of the instance on which the job was run
        session_id integer, -- session identifier of the job run
        slave_pid integer, -- process identifier of the slave on which the job was run
        cpu_used integer, -- amount of cpu used for the job run
        additional_info text -- additional information on the job run, error message, etc.
);
```

## Procedures

### BROKEN

Disables job execution. This procedure sets the broken flag. Broken jobs are never run.

Syntax:

	pg_dbms_job.broken ( 
		job       IN  bigint,
		broken    IN  boolean
		next_date IN  timestamp DEFAULT current_timestamp);

Parameters:

- job : ID of the job being run.
- broken : Sets the job as broken or not broken. `true` sets it as broken; `false` sets it as not broken.
- next_date : Next date when the job will be run, default is `current_timestamp`.

If you set job as broken while it is running, unlike Oracle, the scheduler will not reset the job's status to normal after the job completes. Therefore, you can execute this procedure for jobs when they are running they will be disabled.

Example:

	BEGIN;
	CALL pg_dbms_job.broken(14144, true);
	COMMIT;

### CHANGE

Alters any of the user-definable parameters associated with a job

Syntax:

	dbms_job.change ( 
		job       IN  bigint,
		what      IN  text,
		next_date IN  timestamp with time zone,
		interval  IN  text
		[, instance  IN  integer DEFAULT 0,
		   force     IN  boolean DEFAULT false ]);

Parameters:

- job : ID of the job being run.
- what : PL/SQL procedure to run.
- next_date : Next date when the job will be run.
- interval : Date function; evaluated immediately before the job starts running.
- instance : unused
- force : unused

Your job change will not be available for processing by the job queue in the background until it is committed.
If the parameters what, next_date, or interval are NULL, then leave that value as it is.

Example:

Change the interval of execution of job 14144 to run every 3 days

	BEGIN;
	CALL pg_dbms_job.change(14144, null, null, 'current_timestamp + ''3 days''::interval');
	COMMIT;

### INTERVAL

Alters the interval between executions for a specified job

Syntax:

	dbms_job.interval ( 
		job       IN  bigint,
		interval  IN  text);

Parameters:

- job : ID of the job being run.
- interval : Code of the date function, evaluated immediately before the job starts running.

If the job completes successfully, then this new date is placed in next_date. `interval` is evaluated by plugging it into the statement select interval into next_date;

The interval parameter must evaluate to a time in the future.

If interval evaluates to NULL and if a job completes successfully, then the job is automatically deleted from the queue.

With Oracle this is the kind of interval values that we can find:

- Execute daily: `SYSDATE + 1`
- Execute once per week: `SYSDATE + 7`
- Execute hourly: `SYSDATE + 1/24`
- Execute every 2 hour: `SYSDATE + 2/24`
- Execute every 12 hour: `SYSDATE + 12/24`
- Execute every 10 min.: `SYSDATE + 10/1440`
- Execute every 30 sec.: `SYSDATE + 30/86400`

The equivalent to use with pg_dbms_job are the following:

- Execute daily: `date_trunc('second',LOCALTIMESTAMP) + '1 day'::interval`
- Execute once per week: `date_trunc('second',LOCALTIMESTAMP) + '7 days'::interval` or `date_trunc('second',current_timestamp) + '1 week'::interval`
- Execute hourly: `date_trunc('second',LOCALTIMESTAMP) + '1 hour'::interval`
- Execute every 2 hour: `date_trunc('second',LOCALTIMESTAMP) + '2 hours'::interval`
- Execute every 12 hour: `date_trunc('second',LOCALTIMESTAMP) + '12 hours'::interval`
- Execute every 10 min.: `date_trunc('second',LOCALTIMESTAMP) + '10 minutes'::interval`
- Execute every 30 sec.: `date_trunc('second',LOCALTIMESTAMP) + '30 secondes'::interval`

Example:

	BEGIN;
	CALL pg_dbms_job.interval(14144, 'current_timestamp + '10 seconds'::interval);
	COMMIT;

### NEXT_DATE

Alters the next execution time for a specified job

Syntax:

	dbms_job.next_date ( 
		job       IN  bigint,
		next_date IN  timestamp with time zone);

Parameters:

- job : ID of the job being run.
- next_date : Date of the next refresh: it is when the job will be automatically run, assuming there are background processes attempting to run it.

Example:

	BEGIN;
	CALL pg_dbms_job.next_date(14144, current_timestamp + '1 day'::interval);
	COMMIT;

### REMOVE

Removes specified job from the job queue.

Syntax:

	dbms_job.remove ( 
		job       IN  bigint);

Parameters:

- job : ID of the job being run.

Example:

	BEGIN;
	CALL pg_dbms_job.remove(14144);
	COMMIT;

### RUN

Forces a specified job to run. This procedure runs the job now. It runs even if it is broken. If it was broken and it runs successfully, the job is updated to indicates that it is no longer broken and goes back to running on its schedule.

Running the job recomputes next_date based on the time you run the procedure.

Syntax:

	dbms_job.run ( 
		job       IN  bigint);

Parameters:

- job : ID of the job being run.

Example:

	BEGIN;
	CALL pg_dbms_job.run(14144);
	COMMIT;

### SUBMIT

Submits a new job to the job queue. It chooses the job from the sequence sys.jobseq.

Actually this is a function as PostgreSQL < 14 do not support out parameters.

Syntax

	dbms_job.submit ( 
		job       OUT bigint,
		what      IN  text,
		[ next_date IN  timestamp(0) with time zone DEFAULT current_timestamp
		[ , interval  IN  text DEFAULT NULL
		[ , no_parse  IN  boolean DEFAULT false ] ] ] );

Parameters:

- job : ID of the job being run.
- what : text of the code to the job to be run. This must be a valid SQL statement or block of plpgsql code. The SQL code that you submit in the `what` parameter is wrapped in the following plpgsql block:
```
DO $$
DECLARE
    job bigint := $jobid;
    next_date timestamp with time zone := current_timestamp;
    broken boolean := false;
BEGIN
    WHAT
END;
$$;
```

Ensure that you include the ; semi-colon with the statement.

- next_date : Next date when the job will be run.
- interval : Date function that calculates the next time to run the job. The default is NULL. This must evaluate to a either a future point in time or NULL.
- no_parse : Unused.

Example:

This submits a new job to the job queue. The job calls ANALYZE to generate optimizer statistics for the table public.accounts. The job is run every 24 hours:

	BEGIN;
	DO $$
	DECLARE
	    jobno bigint;
	BEGIN
	   SELECT dbms_job.submit(
	      'ANALYZE public.accounts.',
	      LOCALTIMESTAMP, 'LOCALTIMESTAMP + ''1 day''::interval') INTO jobno;
	END;
	COMMIT;

### WHAT

Alters the job description for a specified job. This procedure changes what an existing job does, and replaces its environment.

Syntax:

	dbms_job.what ( 
		job       IN  bigint,
		what      IN  text);

Parameters:

- job : ID of the job being run. To find this ID, query the JOB column of the USER_JOBS or DBA_JOBS view.
- what : PL/SQL procedure to run.

## Schedule activity on specific intervals

With Oracle this is the kind of interval values that we can find:

- Execute daily: `SYSDATE + 1`
- Execute once per week: `SYSDATE + 7`
- Execute hourly: `SYSDATE + 1/24`
- Execute every 2 hour: `SYSDATE + 2/24`
- Execute every 12 hour: `SYSDATE + 12/24`
- Execute every 10 min.: `SYSDATE + 10/1440`
- Execute every 30 sec.: `SYSDATE + 30/86400`

The equivalent to use with pg_dbms_job are the following:

- Execute daily: `date_trunc('second',LOCALTIMESTAMP) + '1 day'::interval`
- Execute once per week: `date_trunc('second',LOCALTIMESTAMP) + '7 days'::interval` or `date_trunc('second',current_timestamp) + '1 week'::interval`
- Execute hourly: `date_trunc('second',LOCALTIMESTAMP) + '1 hour'::interval`
- Execute every 2 hour: `date_trunc('second',LOCALTIMESTAMP) + '2 hours'::interval`
- Execute every 12 hour: `date_trunc('second',LOCALTIMESTAMP) + '12 hours'::interval`
- Execute every 10 min.: `date_trunc('second',LOCALTIMESTAMP) + '10 minutes'::interval`
- Execute every 30 sec.: `date_trunc('second',LOCALTIMESTAMP) + '30 secondes'::interval`

