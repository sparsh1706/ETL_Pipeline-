-- Create table if it doesn't exist
CREATE TABLE IF NOT EXISTS datamanagementteam16 (
    Fire_ID INTEGER,
    OBJECTID INTEGER,
    SOURCE_REPORTING_UNIT_NAME VARCHAR,
    FIRE_NAME VARCHAR,
    DISCOVERY_DATE DATE,
    DISCOVERY_DOY INTEGER,
    FIRE_YEAR INTEGER,
    STAT_CAUSE_CODE VARCHAR,
    STAT_CAUSE_DESCR VARCHAR,
    CONT_DOY INTEGER,
    CONT_DATE DATE,
    FIRE_SIZE REAL,
    FIRE_SIZE_CLASS VARCHAR,
    LATITUDE REAL,
    LONGITUDE REAL,
    Fire_State VARCHAR,
    Fire_County VARCHAR,
    FIPS_NAME VARCHAR,
    start_date DATE,
    _c0 VARCHAR,
    "Date Local" DATE,
    "CO AQI" REAL,
    "CO Mean" REAL,
    "SO2 AQI" REAL,
    "SO2 Mean" REAL,
    "O3 AQI" REAL,
    "O3 Mean" REAL,
    "NO2 AQI" REAL,
    "NO2 Mean" REAL,
    Final_Latitude REAL,
    Final_Longitude REAL,
    State VARCHAR,
    County VARCHAR,
    Date_Local DATE,
    distance_miles REAL,
    date_diff_fire_pollution INTEGER,
    FIRE_MONTH INTEGER
);

-- Load data from Parquet
COPY datamanagementteam16 FROM '$LOADPATH' (FORMAT PARQUET);

CREATE TEMP TABLE ranked AS
SELECT
    Fire_ID,
    OBJECTID,
    SOURCE_REPORTING_UNIT_NAME,
    FIRE_NAME,
    DISCOVERY_DATE,
    DISCOVERY_DOY,
    FIRE_YEAR,
    STAT_CAUSE_CODE,
    STAT_CAUSE_DESCR,
    CONT_DOY,
    CONT_DATE,
    FIRE_SIZE,
    FIRE_SIZE_CLASS,
    LATITUDE,
    LONGITUDE,
    Fire_State,
    Fire_County,
    FIPS_NAME,
    start_date,
    _c0,
    "Date Local",
    "CO AQI",
    "CO Mean",
    "SO2 AQI",
    "SO2 Mean",
    "O3 AQI",
    "O3 Mean",
    "NO2 AQI",
    "NO2 Mean",
    Final_Latitude,
    Final_Longitude,
    State,
    County,
    Date_Local,
    distance_miles,
    date_diff_fire_pollution,
    FIRE_MONTH,
    ROW_NUMBER() OVER (PARTITION BY start_date, Date_Local ORDER BY "NO2 AQI" DESC) AS rn
FROM datamanagementteam16;

CREATE TEMP TABLE all_data AS
SELECT 
    *
FROM 
    ranked
WHERE 
    rn = 1;

-- Distance-based Analysis
SELECT
  CASE 
    WHEN distance_miles < 10 THEN 'Under 10 miles'
    WHEN distance_miles BETWEEN 10 AND 50 THEN '10-50 miles'
    ELSE 'Over 50 miles'
  END AS distance_category,
    AVG("CO AQI")  AS avg_co_aqi,
    AVG("NO2 AQI") AS avg_no2_aqi,
    AVG("O3 AQI")  AS avg_o3_aqi,
    AVG("SO2 AQI") AS avg_so2_aqi
FROM 
    datamanagementteam16
GROUP BY distance_category
ORDER BY distance_category;

-- Aggregated data
CREATE TEMP TABLE aggregated_fire AS
SELECT
    Fire_ID,
    OBJECTID,
    SOURCE_REPORTING_UNIT_NAME,
    FIRE_NAME,
    DISCOVERY_DATE,
    DISCOVERY_DOY,
    FIRE_YEAR,
    STAT_CAUSE_CODE,
    STAT_CAUSE_DESCR,
    CONT_DOY,
    CONT_DATE,
    FIRE_SIZE,
    FIRE_SIZE_CLASS,
    LATITUDE,
    LONGITUDE,
    Fire_State,
    Fire_County,
    FIPS_NAME,
    start_date,
    _c0,
    "Date Local",
    Final_Latitude,
    Final_Longitude,
    State,
    County,
    Date_Local,
    distance_miles,
    date_diff_fire_pollution,
    FIRE_MONTH,
    AVG(CASE WHEN date_diff_fire_pollution BETWEEN -7 AND -1 
             THEN "SO2 AQI" END) OVER (PARTITION BY start_date) AS avg_SO2_pre,
    AVG(CASE WHEN date_diff_fire_pollution BETWEEN -7 AND -1 
             THEN "NO2 AQI" END) OVER (PARTITION BY start_date) AS avg_NO2_pre,
    AVG(CASE WHEN date_diff_fire_pollution BETWEEN -7 AND -1 
             THEN "CO AQI" END) OVER (PARTITION BY start_date) AS avg_CO_pre,
    AVG(CASE WHEN date_diff_fire_pollution BETWEEN -7 AND -1 
             THEN "O3 AQI" END) OVER (PARTITION BY start_date) AS avg_O3_pre,
    AVG(CASE WHEN date_diff_fire_pollution BETWEEN 1 AND 7 
             THEN "SO2 AQI" END) OVER (PARTITION BY start_date) AS avg_SO2_post,
    AVG(CASE WHEN date_diff_fire_pollution BETWEEN 1 AND 7 
             THEN "NO2 AQI" END) OVER (PARTITION BY start_date) AS avg_NO2_post,
    AVG(CASE WHEN date_diff_fire_pollution BETWEEN 1 AND 7 
             THEN "CO AQI" END) OVER (PARTITION BY start_date) AS avg_CO_post,
    AVG(CASE WHEN date_diff_fire_pollution BETWEEN 1 AND 7 
             THEN "O3 AQI" END) OVER (PARTITION BY start_date) AS avg_O3_post
FROM ranked;

COPY aggregated_fire TO 'aggregated_data.csv' (FORMAT CSV, HEADER TRUE);
COPY all_data TO 'all_data.csv' (FORMAT CSV, HEADER TRUE);


CREATE TEMP TABLE cor AS
  SELECT 
      corr(FIRE_SIZE, avg_CO_post)  AS corr_fire_size_co,
      corr(FIRE_SIZE, avg_NO2_post) AS corr_fire_size_no2,
      corr(FIRE_SIZE, avg_O3_post)  AS corr_fire_size_o3,
      corr(FIRE_SIZE, avg_SO2_post) AS corr_fire_size_so2,
      corr(FIRE_SIZE, (avg_CO_post-avg_CO_pre))  AS corr_fire_size_co_diff,
      corr(FIRE_SIZE, (avg_NO2_post-avg_NO2_pre)) AS corr_fire_size_no2_diff,
      corr(FIRE_SIZE, (avg_O3_post-avg_O3_pre))  AS corr_fire_size_o3_diff,
      corr(FIRE_SIZE, (avg_SO2_post-avg_SO2_pre)) AS corr_fire_size_so2_diff
  FROM aggregated_fire 
;

COPY cor TO 'corr.csv' (FORMAT CSV, HEADER TRUE);
