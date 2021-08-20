-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "CREATE EXTENSION pg_dbms_job" to load this file. \quit

DROP SCHEMA IF EXISTS dbms_job CASCADE; 
CREATE SCHEMA IF NOT EXISTS dbms_job;

CREATE SEQUENCE dbms_job.jobseq;

-- Table used to store the jobs to run by the scheduler
CREATE TABLE dbms_job.all_scheduled_jobs (
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
COMMENT ON TABLE dbms_job.all_scheduled_jobs
    IS 'Table used to store the periodical jobs to run by the scheduler.';
REVOKE ALL ON dbms_job.all_scheduled_jobs FROM PUBLIC;

-- The user can only see the job that he has created
ALTER TABLE dbms_job.all_scheduled_jobs ENABLE ROW LEVEL SECURITY;
CREATE POLICY dbms_job_policy ON dbms_job.all_scheduled_jobs USING (log_user = current_user);

-- Create the asynchronous jobs queue, for immediat execution
CREATE TABLE dbms_job.all_async_jobs (
        job bigint DEFAULT nextval('dbms_job.jobseq') PRIMARY KEY, -- identifier of job
        log_user name DEFAULT current_user, -- user that submit the job
        schema_user text DEFAULT current_setting('search_path'), -- default search_path used to execute the job
        create_date timestamp with time zone DEFAULT current_timestamp, -- date on which this job has been created.
        what text NOT NULL -- body of the anonymous pl/sql block that the job executes
);
COMMENT ON TABLE dbms_job.all_async_jobs
    IS 'Table used to store the jobs to be run asynchronously by the scheduler.';
REVOKE ALL ON dbms_job.all_async_jobs FROM PUBLIC;

-- The user can only see the job that he has created
ALTER TABLE dbms_job.all_async_jobs ENABLE ROW LEVEL SECURITY;
CREATE POLICY dbms_job_policy ON dbms_job.all_async_jobs USING (log_user = current_user);

-- Create a view similar to DMBS_JOB.ALL_JOBS
CREATE VIEW dbms_job.all_jobs AS
    SELECT * FROM dbms_job.all_scheduled_jobs
    UNION
    SELECT job, log_user, NULL priv_user, schema_user, NULL last_date, NULL last_sec,
           NULL this_date, NULL this_sec, create_date next_date, NULL next_sec, NULL total_time,
	   0 broken, NULL "interval", NULL failures, what, NULL nls_env, NULL misc_env,
	   0 instance FROM dbms_job.all_async_jobs;
COMMENT ON VIEW dbms_job.all_jobs
    IS 'View registering all jobs to be run asynchronously or scheduled.';
REVOKE ALL ON dbms_job.all_jobs FROM PUBLIC;

-- Create a table to store the result of the job execution
CREATE TABLE dbms_job.all_scheduler_job_run_details (
	log_id bigserial PRIMARY KEY, -- unique identifier of the log entry
	log_date timestamp with time zone DEFAULT current_timestamp, -- date of the log entry
	owner name, -- owner of the scheduler job
	job_name varchar(261), -- name of the scheduler job
	job_subname varchar(261), -- Subname of the Scheduler job (for a chain step job)
	status text, -- status of the job run
	error char(5), -- error code in the case of an error
	req_start_date timestamp with time zone, -- requested start date of the job run
	actual_start_date timestamp with time zone, -- actual date on which the job was run
	run_duration bigint, -- duration of the job run
	instance_id integer, -- identifier of the instance on which the job was run
	session_id integer, -- session identifier of the job run
	slave_pid integer, -- process identifier of the slave on which the job was run
	cpu_used integer, -- amount of cpu used for the job run
	additional_info	text -- additional information on the job run, error message, etc.
);
COMMENT ON TABLE dbms_job.all_scheduler_job_run_details
    IS 'Table used to store the information about the jobs executed.';
REVOKE ALL ON dbms_job.all_scheduler_job_run_details FROM PUBLIC;

-- The user can only see the job that he has created
ALTER TABLE dbms_job.all_scheduler_job_run_details ENABLE ROW LEVEL SECURITY;
CREATE POLICY dbms_job_policy ON dbms_job.all_scheduler_job_run_details USING (owner = current_user);

----
-- Stored procedures
----
CREATE PROCEDURE dbms_job.broken(
		job       IN  bigint,
		broken    IN  boolean,
		next_date IN  timestamp(0) with time zone DEFAULT current_timestamp)
    LANGUAGE SQL
    AS 'UPDATE dbms_job.all_scheduled_jobs SET broken=$2,next_date=$3 WHERE job=$1';
COMMENT ON PROCEDURE dbms_job.broken(bigint,boolean,timestamp with time zone)
    IS 'Disables job execution. Broken jobs are never run.';
REVOKE ALL ON PROCEDURE dbms_job.broken FROM PUBLIC;

CREATE PROCEDURE dbms_job.change(
		job          IN  bigint,
		what         IN  text,
		next_date    IN  timestamp(0) with time zone,
		job_interval IN  text,
		instance     IN  bigint DEFAULT 0,
		force        IN  boolean DEFAULT false)
    LANGUAGE SQL
    AS 'UPDATE dbms_job.all_scheduled_jobs SET what=$2,next_date=$3,interval=$4 WHERE job=$1';
COMMENT ON PROCEDURE dbms_job.change(bigint,text,timestamp with time zone,text,bigint,boolean)
    IS 'Alters any of the user-definable parameters associated with a job';
REVOKE ALL ON PROCEDURE dbms_job.change FROM PUBLIC;

CREATE PROCEDURE dbms_job.interval(
		job           IN  bigint,
		job_interval  IN  text)
    LANGUAGE SQL
    AS 'UPDATE dbms_job.all_scheduled_jobs SET interval=$2 WHERE job=$1';
COMMENT ON PROCEDURE dbms_job.interval(bigint,text)
    IS 'Alters the interval between executions for a specified job';
REVOKE ALL ON PROCEDURE dbms_job.interval FROM PUBLIC;

CREATE PROCEDURE dbms_job.next_date(
		job        IN  bigint,
		next_date  IN  timestamp(0) with time zone)
    LANGUAGE SQL
    AS 'UPDATE dbms_job.all_scheduled_jobs SET next_date=$2 WHERE job=$1';
COMMENT ON PROCEDURE dbms_job.next_date(bigint,timestamp with time zone)
    IS 'Alters the next execution time for a specified job';
REVOKE ALL ON PROCEDURE dbms_job.next_date FROM PUBLIC;

CREATE PROCEDURE dbms_job.remove(
		job        IN  bigint)
    LANGUAGE SQL
    AS 'DELETE FROM dbms_job.all_scheduled_jobs WHERE job=$1';
COMMENT ON PROCEDURE dbms_job.remove(bigint)
    IS 'Removes specified job from the job queue';
REVOKE ALL ON PROCEDURE dbms_job.remove FROM PUBLIC;

CREATE PROCEDURE dbms_job.run(
		job        IN  bigint)
    LANGUAGE PLPGSQL
    AS $$
DECLARE
    v_what text;
    v_path text;
    start_t timestamp with time zone;
    end_t timestamp with time zone;
BEGIN
    IF job IS NULL THEN
	RETURN;
    END IF;

    -- Get the job definition
    SELECT what, schema_user INTO v_what, v_path FROM dbms_job.all_scheduled_jobs WHERE job = $1;
    start_t :=  clock_timestamp();
    -- Execute the job
    BEGIN
	IF v_path IS NOT NULL THEN
	    EXECUTE 'SET LOCAL search_path TO $1' USING v_path;
	END IF;
	EXECUTE what;
    EXCEPTION
	WHEN others THEN
	    -- Increase the failure count
	    UPDATE dbms_job.all_scheduled_jobs SET failure = failure + 1 WHERE job= $1;
    END;
    end_t :=  clock_timestamp();
    -- Update job's statistics
    UPDATE dbms_job.all_scheduled_jobs SET 
	last_date = end_t,
	last_sec = end_t,
	this_date = start_t,
	this_sec = start_t,
	total_time = total_time + (EXTRACT(EPOCH FROM end_t) - EXTRACT(EPOCH FROM start_t))::bigint
    WHERE job = $1;
END;
$$;
COMMENT ON PROCEDURE dbms_job.run(bigint)
    IS 'Forces a specified job to run immediatly. It runs even if it is broken';
REVOKE ALL ON PROCEDURE dbms_job.run FROM PUBLIC;

CREATE FUNCTION dbms_job.submit(
		jobid         OUT   bigint,
		what          IN    text,
		next_date     IN    timestamp(0) with time zone DEFAULT current_timestamp,
		job_interval  IN    text DEFAULT NULL,
		no_parse      IN    boolean DEFAULT false)
    RETURNS bigint
    LANGUAGE PLPGSQL
    AS $$
BEGIN
    -- When an interval is defined this is a job to be scheduled
    IF job_interval IS NOT NULL THEN
        INSERT INTO dbms_job.all_scheduled_jobs (what,next_date,interval) VALUES ($2,$3,$4) RETURNING job INTO jobid;
    ELSE
	-- With no interval verify if the job is planned in
	-- the future or that it must be executed immediatly
        IF next_date > current_timestamp THEN
            INSERT INTO dbms_job.all_scheduled_jobs (what,next_date,interval) VALUES ($2,$3,$4) RETURNING job INTO jobid;
        ELSE
            -- This is an immediate asynchronous execution, use the special queue
            INSERT INTO dbms_job.all_async_jobs (what) VALUES ($2) RETURNING job INTO jobid;
        END IF;
    END IF;
END;
$$;
COMMENT ON FUNCTION dbms_job.submit(text,timestamp with time zone,text,boolean)
    IS 'Submits a new job to the job queue.';
REVOKE ALL ON FUNCTION dbms_job.submit FROM PUBLIC;

CREATE PROCEDURE dbms_job.what(
		job       IN  bigint,
		what      IN  text)
    LANGUAGE SQL
    AS 'UPDATE dbms_job.all_scheduled_jobs SET what=$2 WHERE job=$1';
COMMENT ON PROCEDURE dbms_job.what(bigint,text)
    IS 'Alters the job description for a specified job';
REVOKE ALL ON PROCEDURE dbms_job.what FROM PUBLIC;

CREATE FUNCTION dbms_job.job_scheduled_notify()
    RETURNS trigger
    LANGUAGE PLPGSQL
    AS $$
BEGIN
    -- Force interval to be NULL if this is set to an empty string
    IF TG_OP = 'UPDATE' OR TG_OP = 'INSERT' THEN
        IF NEW.interval = '' THEN
            NEW.interval := NULL;
        END IF;
    END IF;
    -- When a change occurs in the all_scheduled_jobs table, notify the scheduler.
    IF TG_OP = 'UPDATE' THEN
	-- We do not notify the scheduler if it is at the origine of the UPDATE.
        -- We increment the value of the instance column when this is an internal
	-- update after an execution.
        IF NEW.instance = OLD.instance THEN
	    PERFORM pg_notify('dbms_job_scheduled_notify', TG_OP || ':' || OLD.job || ':' || NEW.job);
        END IF;
	RETURN NEW;
    END IF;
    IF TG_OP = 'INSERT' THEN
	PERFORM pg_notify('dbms_job_scheduled_notify', TG_OP || ':' || NEW.job);
	RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
	PERFORM pg_notify('dbms_job_scheduled_notify', TG_OP || ':' || OLD.job);
	RETURN OLD;
    END IF;
    -- TRUNCATE
    PERFORM pg_notify('dbms_job_scheduled_notify', TG_OP);
    RETURN OLD;
END;
$$;
COMMENT ON FUNCTION dbms_job.job_cache_invalidate()
    IS 'Notify the scheduler that the job cache must be invalidated';

-- When there is a modification in the JOB table invalidate the cache
-- to inform the background worker to reread the table
CREATE TRIGGER dbms_job_scheduled_notify_trg
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
    ON dbms_job.all_scheduled_jobs
    FOR STATEMENT EXECUTE FUNCTION dbms_job.job_scheduled_notify();

CREATE FUNCTION dbms_job.job_async_notify()
    RETURNS trigger
    LANGUAGE PLPGSQL
    AS $$
BEGIN
    -- When a new async job is submitted, notify the scheduler
    PERFORM pg_notify('dbms_job_async_notify', 'New asynchronous job received');
    RETURN NEW;
END;
$$;
COMMENT ON FUNCTION dbms_job.job_async_notify()
    IS 'Notify the scheduler that a new asynchronous job was submitted';

-- When there is a new asynchronous job submited
-- to inform the daemon to reread the table
CREATE TRIGGER dbms_job_async_notify_trg
    AFTER INSERT
    ON dbms_job.all_async_jobs
    FOR STATEMENT EXECUTE FUNCTION dbms_job.job_async_notify();

CREATE FUNCTION dbms_job.get_next_date(text)
    RETURNS timestamp(0) with time zone
    LANGUAGE PLPGSQL
    AS $$
DECLARE
    next_date timestamp(0) with time zone;
BEGIN
	EXECUTE 'SELECT '||$1 INTO next_date;
	RETURN next_date;
END;
$$;
COMMENT ON FUNCTION dbms_job.get_next_date(text)
    IS 'Used to get the next date returned by the interval code';

