# Stop existing Spark session if it exists
try:
    spark.stop()
except NameError:
    pass

# Initialize a new Spark session
import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("JupyterPySpark") \
    .config("spark.driver.host", "localhost") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Define the path to your files
path_fires = r'fire_data_v2.csv'
path_pollution = r'pollution_data_v2.csv'

# Read CSV files into DataFrames
df_fires = spark.read.csv(path_fires, header=True, inferSchema=True)
df_pollution = spark.read.csv(path_pollution, header=True, inferSchema=True)

from pyspark.sql.functions import to_date

# Convert 'Date Local' to a standard date format
df_pollution = df_pollution.withColumn("Date_Local", to_date(df_pollution["Date Local"], "M/d/yy"))

df_pollution = df_pollution.dropna(subset=['Final_Latitude', 'Final_Longitude'])

df_pollution = df_pollution.filter(df_pollution.State.isin(['Arizona', 'California']))

from pyspark.sql.functions import to_date, col
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col
from pyspark.sql.functions import year

state_mapping = {
    'CA': 'California',
    'AZ': 'Arizona'
}

# Convert state abbreviations to full names in df_fires
for abbrev, full_name in state_mapping.items():
    df_fires = df_fires.withColumn("STATE", when(col("STATE") == abbrev, full_name).otherwise(col("STATE")))

df_fires = df_fires.withColumnRenamed("STATE", "Fire_State") \
                   .withColumnRenamed("COUNTY", "Fire_County")

df_fires = df_fires.withColumnRenamed("_c0", "Fire_ID")

df_fires = df_fires.withColumn("DISCOVERY_DATE", to_date("DISCOVERY_DATE"))
df_fires = df_fires.withColumn("CONT_DATE", to_date("CONT_DATE"))

df_pollution = df_pollution.withColumn("Date Local", to_date(df_pollution["Date Local"], "M/d/yy"))

df_joined = df_fires.join(df_pollution, df_fires["Fire_State"] == df_pollution["State"])

df_filtered = df_joined.filter(
    (col("Date_Local") >= col("start_date") - F.expr("INTERVAL 7 DAY")) &
    (col("Date_Local") <= col("start_date") + F.expr("INTERVAL 7 DAY"))
)

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("DistanceFilter").getOrCreate()

# Define Haversine formula function in PySpark
def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance between two points
    on the Earth given their latitude and longitude in degrees.
    Returns distance in miles.
    """
    R = 3958.8  # Radius of Earth in miles
    return (
        F.acos(
            F.sin(F.radians(lat1)) * F.sin(F.radians(lat2)) +
            F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) *
            F.cos(F.radians(lon2 - lon1))
        ) * R
    )

# Apply the Haversine formula
df_filtered = df_filtered.withColumn(
    "distance_miles",
    haversine(F.col("LATITUDE"), F.col("LONGITUDE"), F.col("Final_Latitude"), F.col("Final_Longitude"))
)

# Filter rows where distance is less than 20 miles
filtered_df_20 = df_filtered.filter(F.col("distance_miles") < 20)

filtered_df_20 = filtered_df_20.na.drop(how='any', subset=['CO AQI', 'SO2 AQI'])

from pyspark.sql.functions import datediff

# Convert columns to Date type if necessary
from pyspark.sql.functions import to_date
filtered_df_20 = filtered_df_20.withColumn("start_date", to_date("start_date"))
filtered_df_20 = filtered_df_20.withColumn("Date_Local", to_date("Date_Local"))

# Calculate date difference
filtered_df_20 = filtered_df_20.withColumn("date_diff_fire_pollution", datediff("Date_Local", "start_date"))

# Optionally, extract month from start_date
from pyspark.sql.functions import month

filtered_df_20 = filtered_df_20.withColumn("FIRE_MONTH", month("start_date"))

#selected_columns = [
#    "Fire_ID", "FIRE_YEAR", "STAT_CAUSE_DESCR", "FIRE_SIZE", "FIRE_SIZE_CLASS", 
#    "LATITUDE", "LONGITUDE", "start_date", "CO AQI", "CO Mean", "SO2 AQI", 
#    "SO2 Mean", "O3 AQI", "O3 Mean", "NO2 AQI", "NO2 Mean", "State", "County", 
#    "Date_Local", "distance_miles", "date_diff_fire_pollution"
#]

# Select only these columns

#filtered_df_20 = filtered_df_20.select([col(c) for c in selected_columns])

filtered_years_df = filtered_df_20.filter(filtered_df_20["FIRE_YEAR"].between(2008, 2014))

#filtered_years_df = filtered_df_20.filter(filtered_df_20["start_date"]=='03/04/2014')

filtered_years_df = filtered_years_df.filter((col("start_date") >= "2014-05-02") & (col("start_date") <= "2014-06-02"))

# Write the DataFrame to Parquet
filtered_years_df.write.mode("overwrite").parquet(r"output")

spark.stop()
