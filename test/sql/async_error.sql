-- Insert an asynchronous job
TRUNCATE TABLE dbms_job.all_scheduler_job_run_details;
SET ROLE regress_dbms_job_user;
DO $$
DECLARE
jobid bigint;
BEGIN
	-- Asynchronous job, must be executed immediately
	SELECT dbms_job.submit(
		'INSERT INTO noexisttable VALUES (NULL);' -- what
	) INTO jobid;
END;
$$;
