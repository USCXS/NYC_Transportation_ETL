-- DIMENSION TABLES DEFINE
CREATE OR REPLACE TABLE DIM_DATE_TIME (
    TIME_ID DATE PRIMARY KEY
);

CREATE OR REPLACE TABLE DIM_WEATHER (
    WEATHER_ID INT PRIMARY KEY,
    W_DATE DATE,
    AWND FLOAT,
    PRCP FLOAT,
    TAVG FLOAT
);

CREATE OR REPLACE TABLE DIM_ZONE (
    LOCATIONID INT PRIMARY KEY,
    ZONE VARCHAR(45)
);

CREATE OR REPLACE TABLE DIM_CITIBIKE (
    RIDE_ID VARCHAR(45) PRIMARY KEY,
    RIDEABLE_TYPE VARCHAR(45),
    STARTED_AT TIMESTAMP,
    ENDED_AT TIMESTAMP,
    DURATION FLOAT,
    START_REGION INT,
    START_STATION_NAME VARCHAR(45),
    START_STATION_ID VARCHAR(45),
    END_REGION INT,
    END_STATION_NAME VARCHAR(45),
    END_STATION_ID VARCHAR(45),
    START_LAT FLOAT,
    START_LNG FLOAT,
    END_LAT FLOAT,
    END_LNG FLOAT,
    MEMBER_CASUAL VARCHAR(45)
);

CREATE OR REPLACE TABLE DIM_YELLOW_TAXI (
    YELLOWTAXI_TRIP_ID INT PRIMARY KEY,
    VENDORID INT,
    TPEP_PICKUP_DATETIME TIMESTAMP,
    TPEP_DROPOFF_DATETIME TIMESTAMP,
    DURATION FLOAT,
    PASSENGER_COUNT FLOAT,
    TRIP_DISTANCE FLOAT,
    RATECODEID FLOAT,
    STORE_AND_FWD_FLAG VARCHAR(45),
    PULOCATIONID INT,
    DOLOCATIONID INT,
    PAYMENT_TYPE INT,
    FARE_AMOUNT FLOAT,
    EXTRA FLOAT,
    MTA_TAX FLOAT,
    TIP_AMOUNT FLOAT,
    TOLLS_AMOUNT FLOAT,
    IMPROVEMENT_SURCHARGE FLOAT,
    TOTAL_AMOUNT FLOAT,
    CONGESTION_SURCHARGE FLOAT,
    AIRPORT_FEE FLOAT
);

-- FACT TABLE DEFINE
CREATE OR REPLACE TABLE FACT_TRIP (
    TIME_ID DATE,
    WEATHER_ID INT,
    CITIBIKE_TRIP_ID VARCHAR(45),
    YELLOWTAXI_TRIP_ID INT,
    START_LOCATION_ID INT,
    END_LOCATION_ID INT,
    DURATION_DIFF FLOAT,
    CONSTRAINT PK_TRIPFACT PRIMARY KEY (TIME_ID, WEATHER_ID, CITIBIKE_TRIP_ID, YELLOWTAXI_TRIP_ID, START_LOCATION_ID, END_LOCATION_ID),
    CONSTRAINT FK_TIME_ID FOREIGN KEY (TIME_ID) REFERENCES DIM_DATE_TIME(TIME_ID),
    CONSTRAINT FK_WEATHER_ID FOREIGN KEY (WEATHER_ID) REFERENCES DIM_WEATHER(WEATHER_ID),
    CONSTRAINT FK_CITIBIKE_TRIP_ID FOREIGN KEY (CITIBIKE_TRIP_ID) REFERENCES DIM_CITIBIKE(RIDE_ID),
    CONSTRAINT FK_YELLOWTAXI_TRIP_ID FOREIGN KEY (YELLOWTAXI_TRIP_ID) REFERENCES DIM_YELLOW_TAXI(YELLOWTAXI_TRIP_ID),
    CONSTRAINT FK_START_LOCATION_ID FOREIGN KEY (START_LOCATION_ID) REFERENCES DIM_ZONE(LOCATION_ID),
    CONSTRAINT FK_END_LOCATION_ID FOREIGN KEY (END_LOCATION_ID) REFERENCES DIM_ZONE(LOCATION_ID)
);
