-- Insert an asynchronous job
SET ROLE regress_dbms_job_user;
DO $$
DECLARE
jobid bigint;
BEGIN
	-- Asynchronous job, must be executed immediately
	SELECT dbms_job.submit(
		'BEGIN PERFORM pg_sleep(5); PERFORM current_timestamp; END;' -- what
	) INTO jobid;
END;
$$;
