CREATE OR REPLACE PROCEDURE load_weather_data()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python', 'pandas')
  HANDLER = 'func'
AS
$$
import pandas as pd
import snowflake.snowpark as snowpark
import os

def func(session: snowpark.Session):
    # Define database, schema, stage, and table names
    database_name = 'COURSE_DEMO'
    schema_name = 'PUBLIC'  # Adjust if you have a different schema
    stage_name = 'MY_STAGE'
    file_name = 'nyc_weather_02.csv'
    table_name = f'{database_name}.{schema_name}.WEATHER'

    # Download the file from the stage to a local temporary directory
    temp_dir = '/tmp'
    local_file_path = os.path.join(temp_dir, file_name)
    session.file.get(f"@{stage_name}/{file_name}", temp_dir)

    # Read the file into a Pandas DataFrame
    df = pd.read_csv(local_file_path)

    # Rename columns to match the target table
    df.rename(columns={"DATE": "W_DATE"}, inplace=True)
    
    # Loop through each row in the DataFrame
    for index, row in df.iterrows():
        # Check if a row with the same values exists
        existing_row_query = f"""
        SELECT WEATHER_ID 
        FROM {table_name}
        WHERE W_DATE = '{row['W_DATE']}' 
        AND AWND = {row['AWND']} 
        AND PRCP = {row['PRCP']} 
        AND TAVG = {row['TAVG']}
        """
        existing_row = session.sql(existing_row_query).collect()

        if existing_row:
            # Row with the same values exists, so skip updating or inserting
            continue
        else:
            # Insert the new row, relying on the auto-incrementing WEATHER_ID
            insert_sql = f"""
            INSERT INTO {table_name} (W_DATE, AWND, PRCP, TAVG)
            VALUES ('{row['W_DATE']}', {row['AWND']}, {row['PRCP']}, {row['TAVG']})
            """
            session.sql(insert_sql).collect()

    return "Data loaded successfully, duplicate rows handled with conditional logic"
$$;



CALL load_weather_data();