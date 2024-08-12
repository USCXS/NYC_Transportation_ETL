CREATE OR REPLACE PROCEDURE load_zone_data()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.11'
  PACKAGES = ('snowflake-snowpark-python', 'pandas', 'geopandas', 'shapely')
  HANDLER = 'func'
  EXECUTE AS CALLER
AS
$$
import geopandas as gpd
import pandas as pd
import snowflake.snowpark as snowpark
import os

def func(session: snowpark.Session):
    # Define database, schema, stage, and table names
    database_name = 'COURSE_DEMO'
    schema_name = 'PUBLIC'
    stage_name = 'MY_STAGE'
    file_name = 'taxi_zones.zip'
    table_name = 'ZONE'
    temp_table_name = "TEMP_ZONE"

    # Download the ZIP file from the stage to a local temporary directory
    temp_dir = '/tmp'
    local_file_path = os.path.join(temp_dir, file_name)
    session.file.get(f"@{stage_name}/{file_name}", temp_dir)

    # Read the ZIP file containing shapefiles into a GeoPandas DataFrame
    taxi_zones = gpd.read_file(local_file_path).to_crs("EPSG:4326")

    # Create a list of tuples containing the values to be inserted
    values = []
    for _, row in taxi_zones.iterrows():
        values.append((row['LocationID'], row['zone']))

    # Convert the list of tuples into a Pandas DataFrame
    df_zones = pd.DataFrame(values, columns=['LOCATIONID', 'ZONE'])

    # Write the DataFrame to a temporary Snowflake table
    session.write_pandas(df_zones, temp_table_name, auto_create_table=True, overwrite=True)

    # Perform the merge operation to avoid duplicates
    merge_sql = f"""
    MERGE INTO {table_name} AS target
    USING {temp_table_name} AS source
    ON target.LOCATIONID = source.LOCATIONID
       AND target.ZONE = source.ZONE
    WHEN NOT MATCHED THEN
        INSERT (LOCATIONID, ZONE)
        VALUES (source.LOCATIONID, source.ZONE);
    """

    session.sql(merge_sql).collect()

    # Clean up the temporary table
    session.sql(f"DROP TABLE IF EXISTS {temp_table_name}").collect()

    return "Zone data loaded successfully from ZIP file, duplicates handled with MERGE operation"
$$;

CALL load_zone_data();