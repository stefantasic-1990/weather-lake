import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, col, explode, arrays_zip, to_timestamp, concat_ws, lit, year, month, dayofmonth, hour, col
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

field_rename_map = {
    "pressure_msl (hPa)": "pressure_msl_hpa",
    "precipitation (mm)": "precipitation_mm",
    "temperature_2m (°C)": "temperature_2m_celsius",
    "wind_speed_10m (km/h)": "wind_speed_10m_kmh",
    "relative_humidity_2m (%)": "relative_humidity_2m_percentage",
}
for source_field_name, target_field_name in field_rename_map.items():
    df = df.withColumnRenamed(source_field_name, target_field_name)

df = df.withColumn("raw_object_uri", input_file_name())

df = df.withColumn("location_name", regexp_extract(col("raw_object_uri"), r"location_name=([^/]+)/", 1))

df = df.withColumn("capture_datetime_utc", 
        to_timestamp(
            concat_ws(" ",
                concat_ws(
                    "-",
                    regexp_extract(col("raw_object_uri"), r"year=(\d{4})", 1),
                    regexp_extract(col("raw_object_uri"), r"month=(\d{2})", 1),
                    regexp_extract(col("raw_object_uri"), r"day=(\d{2})", 1)
                ),
                concat_ws(
                    ":",
                    regexp_extract(col("raw_object_uri"), r"hour=(\d{2})", 1),
                    regexp_extract(col("raw_object_uri"), r"minute=(\d{2})", 1),
                    lit("00")
                )
            ),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

df = (df.withColumn("hourly_fields_zipped", arrays_zip("hourly.time", "hourly.temperature_2m"))
       .withColumn("hourly_fields_exploded", explode("hourly_fields_zipped"))
       .withColumn("valid_datetime_utc", to_timestamp(col("hourly_fields_exploded.time")))
       .withColumn("temperature_2m_celsius", col("hourly_fields_exploded.temperature_2m"))
)

df = (
    df
    .withColumn("valid_year", year(col("valid_datetime_utc")))
    .withColumn("valid_month", month(col("valid_datetime_utc")))
    .withColumn("valid_day", dayofmonth(col("valid_datetime_utc")))
    .withColumn("valid_hour", hour(col("valid_datetime_utc")))
)

df = df.select(
    "raw_object_uri",
    "location_name",
    "capture_datetime_utc",
    "valid_datetime_utc",
    "valid_year",
    "valid_month",
    "valid_day",
    "valid_hour",
    "temperature_2m_celsius"
)

df.show(5, truncate=False)

target_write_prefix = f"s3a://{args.weatherlake_bucket_name}/curated/"
df.write.mode("append").partitionBy("location_name", "valid_year", "valid_month", "valid_day") \
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