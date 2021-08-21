-- Insert a scheduled job
TRUNCATE TABLE dbms_job.all_scheduler_job_run_details;
TRUNCATE TABLE dbms_job.all_scheduled_jobs;
SET ROLE regress_dbms_job_user;
DO $$
DECLARE
jobid bigint;
BEGIN
	-- Scheduled job that must be executed 10 seconds
	-- after its creation and then every 10 seconds
	SELECT dbms_job.submit(
		'VACUUM ANALYZE;', -- what
		current_timestamp + '1 day'::interval, -- next_date
		'current_timestamp + ''1 day''::interval' -- interval
	) INTO jobid;
END;
$$;
