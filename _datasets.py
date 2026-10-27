from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("StreamFromKafkaToParquet") \
    .getOrCreate()

df = spark.read.parquet(
    "s3a://sink/transactions/2026-10-25--21/"
)

df.show(truncate=False)
print("Total rows:", df.count())