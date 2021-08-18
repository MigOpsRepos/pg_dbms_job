-- Insert a scheduled job and an asynchronous one
DO $$
DECLARE
jobid bigint;
BEGIN
	-- Scheduled job that must be executed 10 seconds
	-- after its creation and then every 10 seconds
	SELECT dbms_job.submit(
		'ANALYZE', -- what
		current_timestamp + '10 seconds'::interval, -- next_date
		'current_timestamp + ''1 day''::interval' -- interval
	) INTO jobid;
	RAISE NOTICE 'JOBID: %', jobid;

	-- Asynchronous job, must be executed immediately
	SELECT dbms_job.submit(
		'SELECT pg_sleep(10);ANALYZE dbms_job.all_scheduled_jobs;' -- what
	) INTO jobid;
	RAISE NOTICE 'JOBID: %', jobid;
END;
$$;
