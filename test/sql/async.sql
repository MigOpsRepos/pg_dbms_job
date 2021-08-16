-- Insert an asynchronous job
SET ROLE regress_dbms_job_user;
DO $$
DECLARE
jobid bigint;
BEGIN
	-- Asynchronous job, must be executed immediately
	SELECT dbms_job.submit(
		'SELECT pg_sleep(5); SELECT current_timestamp;' -- what
	) INTO jobid;
END;
$$;
