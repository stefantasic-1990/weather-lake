from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Weather-Lake-Load").getOrCreate()

input_path = (
    "s3a://weather-lake-raw-archive/Toronto/year=2025/month=10/day=11/"
    "openmeteo-16day-hourly-forecast_1425_utc.csv"
)

df = spark.read.option("header", True).csv(input_path)

output_path = "s3a://weather-lake/sample.parquet"
df.write.mode("overwrite").parquet(output_path)

spark.stop()