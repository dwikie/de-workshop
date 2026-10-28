from pyflink.table import EnvironmentSettings, TableEnvironment

settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(settings)

t_env.execute_sql("""
    CREATE TEMPORARY TABLE curated_transactions (
        transaction_id STRING,
        account_id STRING,
        amount DECIMAL(18, 2),
        currency STRING,
        event_time STRING,
        country STRING,
        status STRING,
        event_date DATE
    ) WITH (
        'connector' = 'filesystem',
        'path' = 's3a://warehouse/transaction/',
        'format' = 'parquet'
    )
""")

t_env.execute_sql("""
    SELECT transaction_id, event_time, event_date
    FROM curated_transactions
""").print()