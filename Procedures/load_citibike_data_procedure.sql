CREATE OR REPLACE PROCEDURE load_citibike_data()
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
import zipfile

def func(session: snowpark.Session):
    # Define database, schema, stage, and table names
    database_name = 'COURSE_DEMO'
    schema_name = 'PUBLIC'
    stage_name = 'MY_STAGE'
    file_name = '202302-citibike-tripdata.zip'
    taxi_zones_file_name = 'taxi_zones.zip'
    table_name = 'CITIBIKE'
    temp_dir = '/tmp'

    # Download the ZIP file from the stage to a local temporary directory
    local_file_path = os.path.join(temp_dir, file_name)
    session.file.get(f"@{stage_name}/{file_name}", temp_dir)

    # Unzip the Citibike data files
    with zipfile.ZipFile(local_file_path, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)

    # Load the Citibike CSV data into Pandas DataFrames
    citibike_csv_path_1 = os.path.join(temp_dir, '202302-citibike-tripdata_1.csv')
    citibike_csv_path_2 = os.path.join(temp_dir, '202302-citibike-tripdata_2.csv')
    df1 = pd.read_csv(citibike_csv_path_1, nrows=2000)
    df2 = pd.read_csv(citibike_csv_path_2, nrows=2000)

    # Concatenate the two DataFrames
    df = pd.concat([df1, df2], ignore_index=True)

    # Convert all column names to uppercase
    df.columns = df.columns.str.upper()

    df['STARTED_AT'] = pd.to_datetime(df['STARTED_AT'])
    df['ENDED_AT'] = pd.to_datetime(df['ENDED_AT'])
    df['DURATION'] = (df['ENDED_AT'] - df['STARTED_AT']).dt.total_seconds() / 60
    df['DURATION'] = df['DURATION'].round(1)
    df['STARTED_AT'] = df['STARTED_AT'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['ENDED_AT'] = df['ENDED_AT'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Load the taxi zones shapefile for spatial mapping
    taxi_zones_local_file_path = os.path.join(temp_dir, taxi_zones_file_name)
    session.file.get(f"@{stage_name}/{taxi_zones_file_name}", temp_dir)
    taxi_zones = gpd.read_file(taxi_zones_local_file_path).to_crs("EPSG:4326")

    # Create GeoDataFrames for both start and end stations
    start_gdf = gpd.GeoDataFrame(
        df,
        crs="EPSG:4326",
        geometry=gpd.points_from_xy(df["START_LNG"], df["START_LAT"])
    )

    end_gdf = gpd.GeoDataFrame(
        df,
        crs="EPSG:4326",
        geometry=gpd.points_from_xy(df["END_LNG"], df["END_LAT"])
    )

    # Perform a spatial join for start stations to map them to taxi regions
    start_mapped = gpd.sjoin(start_gdf, taxi_zones, how="inner")
    df['START_REGION'] = start_mapped['LocationID']

    # Perform a spatial join for end stations to map them to taxi regions
    end_mapped = gpd.sjoin(end_gdf, taxi_zones, how="inner")
    df['END_REGION'] = end_mapped['LocationID']

    # Write the DataFrame to the Snowflake table
    session.write_pandas(df, table_name, auto_create_table=False, overwrite=False)

    return "Citibike data loaded successfully from ZIP file and mapped to taxi regions"
$$;


CALL load_citibike_data();