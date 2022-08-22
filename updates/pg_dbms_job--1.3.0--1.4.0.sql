CREATE OR REPLACE PROCEDURE dbms_job.interval(
		jobid           IN  bigint,
		job_interval  IN  text)
    LANGUAGE PLPGSQL 
    AS $$
DECLARE
    next_date timestamp with time zone;
    v_interval text;
    v_retval bigint;
BEGIN
    IF job_interval IS NULL THEN
        UPDATE dbms_job.all_scheduled_jobs SET interval = NULL WHERE job = jobid;
    ELSE
        -- interval must be in the future
        next_date := dbms_job.get_next_date(job_interval);
        IF next_date < current_timestamp THEN
    	    RAISE EXCEPTION 'Interval must evaluate to a time in the future: %', next_date USING ERRCODE = '23420';
        END IF;
        v_interval := 'UPDATE dbms_job.all_scheduled_jobs SET interval = ' || quote_literal(job_interval) || ' WHERE job = ' || jobid || ' RETURNING job';
        EXECUTE v_interval INTO v_retval;
        IF v_retval IS NULL THEN
            RAISE EXCEPTION 'null_value_not_allowed' USING detail = 'job number is not a job in the job queue';
        END IF;
    END IF;
END;
$$;
