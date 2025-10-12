import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, col
from pyspark.sql.types import IntegerType

parser = argparse.ArgumentParser()
parser.add_argument("--raw-bucket", required=True)
parser.add_argument("--warehouse-bucket", required=True)
parser.add_argument("--object-keys", required=True)
args = parser.parse_args()

object_keys = args.object_keys.split(",")
object_uris = [f"s3a://{args.raw_bucket}/{key}" for key in object_keys]

spark = SparkSession.builder.appName("Weather-Lake-Load").getOrCreate()

df = spark.read.option("header", True).csv(object_uris)
df = (
    df.withColumn("input_file", input_file_name())
      .withColumn("location_name", regexp_extract("input_file", r"location_name=([^/]+)/", 1))
      .withColumn("year", regexp_extract("input_file", r"year=(\d{4})", 1).cast(IntegerType()))
      .withColumn("month", regexp_extract("input_file", r"month=(\d{2})", 1).cast(IntegerType()))
      .withColumn("day", regexp_extract("input_file", r"day=(\d{2})", 1).cast(IntegerType()))
)

df.write.mode("append").partitionBy("location_name", "year", "month", "day") \
    .parquet(f"s3a://{args.warehouse_bucket}/curated/")

spark.stop()