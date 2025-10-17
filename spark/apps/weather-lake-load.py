import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, col
from pyspark.sql.types import IntegerType

parser = argparse.ArgumentParser()
parser.add_argument("--weatherlake-bucket-name", required=True)
parser.add_argument("--forecast-object-keys", required=True)
args = parser.parse_args()

forecast_object_keys = args.forecast_object_keys.split(",")
object_uris = [f"s3a://{args.weatherlake_bucket_name}/{object_key}" for object_key in forecast_object_keys]

spark = (
    SparkSession.builder
    .appName("weather-lake-load")
    .enableHiveSupport()
    .getOrCreate()
)

df = spark.read.option("header", True).json(object_uris)

df = (
    df.withColumn("input_file", input_file_name())
      .withColumn("location_name", regexp_extract("input_file", r"location_name=([^/]+)/", 1))
      .withColumn("year", regexp_extract("input_file", r"year=(\d{4})", 1).cast(IntegerType()))
      .withColumn("month", regexp_extract("input_file", r"month=(\d{2})", 1).cast(IntegerType()))
      .withColumn("day", regexp_extract("input_file", r"day=(\d{2})", 1).cast(IntegerType()))
)

field_rename_map = {
    "time": "forecast_datetime_utc",
    "pressure_msl (hPa)": "pressure_msl_hpa",
    "precipitation (mm)": "precipitation_mm",
    "temperature_2m (°C)": "temperature_2m_celsius",
    "wind_speed_10m (km/h)": "wind_speed_10m_kmh",
    "relative_humidity_2m (%)": "relative_humidity_2m_percentage",
}

for source_field_name, target_field_name in field_rename_map.items():
    df = df.withColumnRenamed(source_field_name, target_field_name)

target_write_prefix = f"s3a://{args.weatherlake_bucket_name}/curated/"
df.write.mode("append").partitionBy("location_name", "year", "month", "day") \
    .parquet(target_write_prefix)

spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS weather_forecast (
        forecast_datetime_utc STRING,
        pressure_msl_hpa DOUBLE,
        precipitation_mm DOUBLE,
        temperature_2m_celsius DOUBLE,
        wind_speed_10m_kmh DOUBLE,
        relative_humidity_2m_percentage DOUBLE
    )
    PARTITIONED BY (location_name STRING, year INT, month INT, day INT)
    STORED AS PARQUET
    LOCATION '{target_write_prefix}'
""")

spark.sql("MSCK REPAIR TABLE weather_forecast")

spark.stop()