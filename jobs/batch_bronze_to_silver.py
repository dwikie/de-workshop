
from pyspark.sql import SparkSession, functions as F

BRONZE_PATH = "s3a://bronze/transactions/"
TABLE = "silver.datalake.transactions"

spark = (
    SparkSession.builder
    .appName("bronze-to-silver-initial-load")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

try:
    # 1. Read all Bronze Parquet files, including nested folders.
    bronze = (
        spark.read
        .option("recursiveFileLookup", "true")
        .parquet(BRONZE_PATH)
    )

    # 2. Normalize the event timestamp.
    # The source values are expected to be ISO-8601 strings.
    source = bronze.withColumn(
        "event_time",
        F.to_timestamp(
            F.col("event_time"),
            "yyyy-MM-dd'T'HH:mm:ssX"
        )
    )

    # 3. Fail loudly on invalid required fields.
    invalid = source.filter(
        F.col("transaction_id").isNull()
        | F.col("event_time").isNull()
    )

    if invalid.limit(1).count() > 0:
        raise ValueError(
            "Bronze contains missing transaction IDs "
            "or invalid event timestamps."
        )

    # 4. Do not arbitrarily choose between conflicting duplicates.
    duplicates = (
        source.groupBy("transaction_id")
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicates.limit(1).count() > 0:
        raise ValueError(
            "Duplicate transaction IDs found. "
            "Define the deduplication/business-key rule first."
        )

    source.createOrReplaceTempView("silver_source")

    # 5. Create the namespace and table at the explicit Silver path.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS silver.datalake")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            transaction_id STRING,
            account_id STRING,
            amount DECIMAL(18,2),
            currency STRING,
            event_time TIMESTAMP,
            country STRING,
            status STRING
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.target-file-size-bytes' = '134217728',
            'write.distribution-mode' = 'hash'
        )
    """)

    # 6. Initial load only. Do not rerun this append blindly.
    existing_count = spark.table(TABLE).count()

    if existing_count != 0:
        raise RuntimeError(
            f"{TABLE} already contains {existing_count} rows. "
            "Use a MERGE-based incremental job instead."
        )

    source.select(
        "transaction_id",
        "account_id",
        "amount",
        "currency",
        "event_time",
        "country",
        "status"
    ).writeTo(TABLE).append()

    # 7. Verify the write.
    result = spark.table(TABLE)

    result.printSchema()
    result.show(20, truncate=False)

    print(f"SILVER_ROW_COUNT={result.count()}")
    print(f"SILVER_TABLE={TABLE}")

    print("BRONZE_TO_SILVER_INITIAL_LOAD=PASS")

finally:
    spark.stop()
