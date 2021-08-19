-- Insert an asynchronous job that must be executed later
TRUNCATE TABLE dbms_job.all_scheduler_job_run_details;
SET ROLE regress_dbms_job_user;
DO $$
DECLARE
jobid bigint;
BEGIN
	-- Asynchronous job, must be executed immediately, there
	-- is no interval and next_date is set to current timestamp
	SELECT dbms_job.submit(
		'BEGIN PERFORM current_timestamp; END;', -- what
		LOCALTIMESTAMP + '3 seconds'::interval, NULL
	) INTO jobid;
END;
$$;
