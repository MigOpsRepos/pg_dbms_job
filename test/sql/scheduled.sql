-- Insert an asynchronous job that must be executed later
TRUNCATE TABLE dbms_job.all_scheduler_job_run_details;
SET ROLE regress_dbms_job_user;
DO $$
DECLARE
jobid bigint;
BEGIN
	-- Scheduled job that will be executed in 3 seconds
	-- and each 10 seconds after that.
	SELECT dbms_job.submit(
		'BEGIN PERFORM LOCALTIMESTAMP; END;', -- what
		LOCALTIMESTAMP + '3 seconds'::interval, -- start the job in 3 seconds
		'LOCALTIMESTAMP + ''6 seconds''::interval' -- repeat the job every 6 seconds
	) INTO jobid;
END;
$$;
