CREATE OR REPLACE PROCEDURE load_yellow_taxi_data()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python', 'pandas')
  HANDLER = 'func'
  EXECUTE AS CALLER
AS
$$
import pandas as pd
import snowflake.snowpark as snowpark
import os

def func(session: snowpark.Session):
    # Define database, schema, stage, and table names
    database_name = 'COURSE_DEMO'
    schema_name = 'PUBLIC'
    stage_name = 'MY_STAGE'
    file_name = 'yellow_tripdata_2023-02.parquet'
    table_name = 'YELLOW_TAXI'
    temp_table_name = "TEMP_YELLOW_TAXI"

    # Download the file from the stage to a local temporary directory
    temp_dir = '/tmp'
    local_file_path = os.path.join(temp_dir, file_name)
    session.file.get(f"@{stage_name}/{file_name}", temp_dir)

    # Read the Parquet file into a Pandas DataFrame
    df_yt = pd.read_parquet(local_file_path).head(2000)

    # Capitalize all column names
    df_yt.columns = [col.upper() for col in df_yt.columns]

    # Calculate duration in minutes and round it
    df_yt['DURATION'] = (df_yt['TPEP_DROPOFF_DATETIME'] - df_yt['TPEP_PICKUP_DATETIME']).dt.total_seconds() / 60
    df_yt['DURATION'] = df_yt['DURATION'].round(1)
    
    # Format datetime columns
    df_yt['TPEP_PICKUP_DATETIME'] = df_yt['TPEP_PICKUP_DATETIME'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_yt['TPEP_DROPOFF_DATETIME'] = df_yt['TPEP_DROPOFF_DATETIME'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Ensure uniqueness in the DataFrame
    df_yt.drop_duplicates(inplace=True)

    # Write the DataFrame to a Snowflake temporary table
    session.write_pandas(df_yt, temp_table_name, auto_create_table=True, overwrite=True)

    # Perform the merge operation
    merge_sql = f"""
    MERGE INTO {table_name} AS target
    USING {temp_table_name} AS source
    ON target.TPEP_PICKUP_DATETIME = source.TPEP_PICKUP_DATETIME
       AND target.TPEP_DROPOFF_DATETIME = source.TPEP_DROPOFF_DATETIME
       AND target.VENDORID = source.VENDORID
       AND target.DURATION = source.DURATION
       AND target.PASSENGER_COUNT = source.PASSENGER_COUNT
       AND target.TRIP_DISTANCE = source.TRIP_DISTANCE
       AND target.RATECODEID = source.RATECODEID
       AND target.PULOCATIONID = source.PULOCATIONID
       AND target.DOLOCATIONID = source.DOLOCATIONID
       AND target.PAYMENT_TYPE = source.PAYMENT_TYPE
       AND target.FARE_AMOUNT = source.FARE_AMOUNT
       AND target.EXTRA = source.EXTRA
       AND target.MTA_TAX = source.MTA_TAX
       AND target.TIP_AMOUNT = source.TIP_AMOUNT
       AND target.TOLLS_AMOUNT = source.TOLLS_AMOUNT
       AND target.IMPROVEMENT_SURCHARGE = source.IMPROVEMENT_SURCHARGE
       AND target.TOTAL_AMOUNT = source.TOTAL_AMOUNT
       AND target.CONGESTION_SURCHARGE = source.CONGESTION_SURCHARGE
       AND target.AIRPORT_FEE = source.AIRPORT_FEE
    WHEN NOT MATCHED THEN
        INSERT (VENDORID, TPEP_PICKUP_DATETIME, TPEP_DROPOFF_DATETIME, DURATION,
                PASSENGER_COUNT, TRIP_DISTANCE, RATECODEID, STORE_AND_FWD_FLAG, PULOCATIONID,
                DOLOCATIONID, PAYMENT_TYPE, FARE_AMOUNT, EXTRA, MTA_TAX, TIP_AMOUNT,
                TOLLS_AMOUNT, IMPROVEMENT_SURCHARGE, TOTAL_AMOUNT, CONGESTION_SURCHARGE, AIRPORT_FEE)
        VALUES (source.VENDORID, source.TPEP_PICKUP_DATETIME, source.TPEP_DROPOFF_DATETIME, source.DURATION,
                source.PASSENGER_COUNT, source.TRIP_DISTANCE, source.RATECODEID, source.STORE_AND_FWD_FLAG, source.PULOCATIONID,
                source.DOLOCATIONID, source.PAYMENT_TYPE, source.FARE_AMOUNT, source.EXTRA, source.MTA_TAX, source.TIP_AMOUNT,
                source.TOLLS_AMOUNT, source.IMPROVEMENT_SURCHARGE, source.TOTAL_AMOUNT, source.CONGESTION_SURCHARGE, source.AIRPORT_FEE);
    """

    session.sql(merge_sql).collect()

    # Clean up the temporary table
    session.sql(f"DROP TABLE IF EXISTS {temp_table_name}").collect()

    return "Yellow Taxi data loaded successfully, duplicates handled efficiently with MERGE operation"
$$;


CALL load_yellow_taxi_data();