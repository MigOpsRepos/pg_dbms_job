-- Insert an asynchronous job
TRUNCATE TABLE dbms_job.all_scheduler_job_run_details;
SET ROLE regress_dbms_job_user;
DO $$
DECLARE
jobid bigint;
BEGIN
	-- Asynchronous job, must be executed immediately, there
	-- is no interval and next_date is set to current timestamp
	SELECT dbms_job.submit(
		'BEGIN INSERT INTO noexisttable VALUES (NULL); END;', -- what
		LOCALTIMESTAMP, NULL
	) INTO jobid;
END;
$$;
