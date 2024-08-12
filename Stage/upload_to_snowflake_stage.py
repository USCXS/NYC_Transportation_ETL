import snowflake.connector
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'COURSE_DEMO'
SNOWFLAKE_SCHEMA = 'PUBLIC'

# Establish a Snowflake connection
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)

# https://www.ncei.noaa.gov/cdo-web/search

try:
    # Create a cursor object
    cur = conn.cursor()

    # Step 1: Create or replace a stage
    cur.execute("""
    CREATE OR REPLACE STAGE MY_STAGE
    FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY='"')
    """)
    print("Stage created successfully.")

    cur.execute("""
    PUT file://C:\\Users\\19516\\Desktop\\Orchestration_Pipeline_Demo\\Code\\NYC_Transportation_ETL_Pipeline\\data\\nyc_weather_02.csv @MY_STAGE AUTO_COMPRESS=FALSE
    """)
    print("Weather file uploaded successfully.")

    # Upload local ZIP files to the stage
    cur.execute("""
    PUT file://C:\\Users\\19516\\Desktop\\Orchestration_Pipeline_Demo\\Code\\NYC_Transportation_ETL_Pipeline\\data\\202302-citibike-tripdata.zip @MY_STAGE AUTO_COMPRESS=FALSE
    """)
    print("Local ZIP file uploaded successfully.")

    cur.execute("""
    PUT file://C:\\Users\\19516\\Desktop\\Orchestration_Pipeline_Demo\\Code\\NYC_Transportation_ETL_Pipeline\\data\\taxi_zones.zip @MY_STAGE AUTO_COMPRESS=FALSE
    """)
    print("Taxi zones file uploaded successfully.")

    cur.execute("""
    PUT file://C:\\Users\\19516\\Desktop\\Orchestration_Pipeline_Demo\\Code\\NYC_Transportation_ETL_Pipeline\\data\\yellow_tripdata_2023-02.parquet @MY_STAGE AUTO_COMPRESS=FALSE
    """)
    print("Yellow Tripdata file uploaded successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Always close the cursor and connection
    cur.close()
    conn.close()
