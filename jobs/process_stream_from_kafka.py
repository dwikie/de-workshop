
from pyflink.table import EnvironmentSettings, TableEnvironment

RAW_PATH = "s3a://sink/transactions/"
WAREHOUSE_PATH = "s3a://warehouse/transaction/"


def main():
    settings = EnvironmentSettings.in_batch_mode()
    table_env = TableEnvironment.create(settings)

    table_env.execute_sql(f"""
        CREATE TEMPORARY TABLE raw_transactions (
            transaction_id STRING,
            account_id STRING,
            amount DECIMAL(18, 2),
            currency STRING,
            event_time STRING,
            country STRING,
            status STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{RAW_PATH}',
            'format' = 'parquet'
        )
    """)

    table_env.execute_sql(f"""
        CREATE TEMPORARY TABLE curated_transactions (
            transaction_id STRING,
            account_id STRING,
            amount DECIMAL(18, 2),
            currency STRING,
            event_time STRING,
            country STRING,
            status STRING,
            event_date DATE
        ) PARTITIONED BY (event_date) WITH (
            'connector' = 'filesystem',
            'path' = '{WAREHOUSE_PATH}',
            'format' = 'parquet'
        )
    """)

    result = table_env.execute_sql("""
        INSERT OVERWRITE curated_transactions
        SELECT
            transaction_id,
            account_id,
            amount,
            currency,
            event_time,
            country,
            status,
            CAST(
                TO_TIMESTAMP(
                    REPLACE(SUBSTRING(event_time, 1, 19), 'T', ' '),
                    'yyyy-MM-dd HH:mm:ss'
                ) AS DATE
            ) AS event_date
        FROM raw_transactions
        WHERE event_time IS NOT NULL
    """)

    result.wait()


if __name__ == "__main__":
    main()
