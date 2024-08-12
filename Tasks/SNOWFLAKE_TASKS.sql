-- Create a single task for loading all necessary data before the datamart
CREATE OR REPLACE TASK TASK_LOAD_ALL_DATA
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON * * * * * UTC' -- Adjust the schedule as needed
AS
BEGIN
  -- Call each procedure in sequence
  CALL LOAD_ZONE_DATA();
  CALL LOAD_CITIBIKE_DATA();
  CALL LOAD_YELLOW_TAXI_DATA();
  CALL LOAD_WEATHER_DATA();
END;

-- Task for loading data into the datamart (depends on the combined task)
CREATE OR REPLACE TASK TASK_LOAD_DATA_INTO_DATAMART
  WAREHOUSE = COMPUTE_WH
  AFTER TASK_LOAD_ALL_DATA
AS
  CALL LOAD_DATA_INTO_DATAMART();

-- Enable the tasks to start the sequence
ALTER TASK TASK_LOAD_ALL_DATA RESUME;
ALTER TASK TASK_LOAD_DATA_INTO_DATAMART RESUME;

-- Suspend Tasks
ALTER TASK TASK_LOAD_ALL_DATA SUSPEND;
ALTER TASK TASK_LOAD_DATA_INTO_DATAMART SUSPEND;