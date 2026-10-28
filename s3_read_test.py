
from pyspark.sql import SparkSession

BRONZE_PATH = "s3a://bronze/transactions/"

spark = (
    SparkSession.builder
    .appName("spark-hive-seaweedfs-integration-test")
    .getOrCreate()
)

try:
    print("=== TEST 1: Hive Metastore ===")
    databases = spark.sql("SHOW DATABASES IN hive_prod")
    databases.show(truncate=False)
    print("HIVE_CATALOG_TEST=PASS")

    print("=== TEST 2: SeaweedFS Bronze ===")
    path = spark._jvm.org.apache.hadoop.fs.Path(BRONZE_PATH)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())

    if not fs.exists(path):
        raise RuntimeError(f"Bronze path does not exist: {BRONZE_PATH}")

    files = fs.listStatus(path)
    print(f"BRONZE_PATH={BRONZE_PATH}")
    print(f"TOP_LEVEL_ENTRIES={len(files)}")

    # Recursively read Parquet files, including Flink's time-based folders.
    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .parquet(BRONZE_PATH)
    )

    print("=== BRONZE SCHEMA ===")
    df.printSchema()

    print("=== SAMPLE RECORDS ===")
    df.show(5, truncate=False)

    print(f"BRONZE_RECORD_COUNT={df.count()}")
    print("SEAWEEDFS_PARQUET_TEST=PASS")

finally:
    spark.stop()
